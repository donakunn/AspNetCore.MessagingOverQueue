using MessagingOverQueue.Abstractions.Consuming;
using MessagingOverQueue.Abstractions.Messages;
using MessagingOverQueue.Configuration.Options;
using MessagingOverQueue.Connection;
using MessagingOverQueue.Consuming.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MessagingOverQueue.Consuming;

/// <summary>
/// RabbitMQ implementation of message consumer.
/// </summary>
public class RabbitMqConsumer : IMessageConsumer
{
    private readonly IRabbitMqConnectionPool _connectionPool;
    private readonly IServiceProvider _serviceProvider;
    private readonly ConsumerOptions _options;
    private readonly IEnumerable<IConsumeMiddleware> _middlewares;
    private readonly ILogger<RabbitMqConsumer> _logger;
    
    private IChannel? _channel;
    private string? _consumerTag;
    private readonly SemaphoreSlim _concurrencySemaphore;
    private volatile bool _isRunning;
    private readonly CancellationTokenSource _stoppingCts = new();

    public RabbitMqConsumer(
        IRabbitMqConnectionPool connectionPool,
        IServiceProvider serviceProvider,
        ConsumerOptions options,
        IEnumerable<IConsumeMiddleware> middlewares,
        ILogger<RabbitMqConsumer> logger)
    {
        _connectionPool = connectionPool;
        _serviceProvider = serviceProvider;
        _options = options;
        _middlewares = middlewares;
        _logger = logger;
        _concurrencySemaphore = new SemaphoreSlim(options.MaxConcurrency, options.MaxConcurrency);
    }

    public bool IsRunning => _isRunning;

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_isRunning)
            return;

        _logger.LogInformation("Starting consumer for queue '{Queue}' with prefetch {Prefetch}",
            _options.QueueName, _options.PrefetchCount);

        _channel = await _connectionPool.CreateDedicatedChannelAsync(cancellationToken);
        await _channel.BasicQosAsync(0, _options.PrefetchCount, false, cancellationToken);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += OnMessageReceivedAsync;

        _consumerTag = await _channel.BasicConsumeAsync(
            queue: _options.QueueName,
            autoAck: _options.AutoAck,
            consumerTag: _options.ConsumerTag ?? $"consumer-{Guid.NewGuid():N}",
            consumer: consumer,
            cancellationToken: cancellationToken);

        _isRunning = true;
        _logger.LogInformation("Consumer started with tag '{ConsumerTag}'", _consumerTag);
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (!_isRunning)
            return;

        _logger.LogInformation("Stopping consumer '{ConsumerTag}'", _consumerTag);
        
        await _stoppingCts.CancelAsync();

        if (_channel != null && _consumerTag != null)
        {
            try
            {
                await _channel.BasicCancelAsync(_consumerTag, cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error canceling consumer");
            }
        }

        _isRunning = false;
        _logger.LogInformation("Consumer stopped");
    }

    private async Task OnMessageReceivedAsync(object sender, BasicDeliverEventArgs args)
    {
        await _concurrencySemaphore.WaitAsync(_stoppingCts.Token);
        
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_stoppingCts.Token);
            cts.CancelAfter(_options.ProcessingTimeout);

            await ProcessMessageAsync(args, cts.Token);
        }
        catch (OperationCanceledException) when (_stoppingCts.IsCancellationRequested)
        {
            _logger.LogDebug("Message processing cancelled due to shutdown");
            if (!_options.AutoAck && _channel != null)
            {
                await _channel.BasicNackAsync(args.DeliveryTag, false, true);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing message, delivery tag: {DeliveryTag}", args.DeliveryTag);
            if (!_options.AutoAck && _channel != null)
            {
                await _channel.BasicNackAsync(args.DeliveryTag, false, _options.RequeueOnFailure);
            }
        }
        finally
        {
            _concurrencySemaphore.Release();
        }
    }

    private async Task ProcessMessageAsync(BasicDeliverEventArgs args, CancellationToken cancellationToken)
    {
        var headers = args.BasicProperties.Headers?
            .ToDictionary(
                x => x.Key, 
                x => x.Value is byte[] bytes ? System.Text.Encoding.UTF8.GetString(bytes) : x.Value)
            ?? [];

        var messageContext = new MessageContext(
            messageId: Guid.TryParse(args.BasicProperties.MessageId, out var id) ? id : Guid.NewGuid(),
            queueName: _options.QueueName,
            correlationId: args.BasicProperties.CorrelationId,
            exchangeName: args.Exchange,
            routingKey: args.RoutingKey,
            headers: headers!,
            deliveryCount: args.Redelivered ? 2 : 1);

        var context = new ConsumeContext
        {
            Body = args.Body.ToArray(),
            MessageContext = messageContext,
            DeliveryTag = args.DeliveryTag,
            Redelivered = args.Redelivered,
            Headers = headers!,
            ContentType = args.BasicProperties.ContentType
        };

        var pipeline = new ConsumePipeline(_middlewares, HandleMessageAsync);
        await pipeline.ExecuteAsync(context, cancellationToken);

        if (!_options.AutoAck && _channel != null)
        {
            if (context.ShouldReject)
            {
                await _channel.BasicRejectAsync(args.DeliveryTag, context.RequeueOnReject, cancellationToken);
            }
            else if (context.ShouldAck)
            {
                await _channel.BasicAckAsync(args.DeliveryTag, false, cancellationToken);
            }
        }
    }

    private async Task HandleMessageAsync(ConsumeContext context, CancellationToken cancellationToken)
    {
        if (context.Message == null || context.MessageType == null)
        {
            _logger.LogWarning("Message was not deserialized, skipping handler invocation");
            return;
        }

        using var scope = _serviceProvider.CreateScope();
        
        var handlerType = typeof(IMessageHandler<>).MakeGenericType(context.MessageType);
        var handlers = scope.ServiceProvider.GetServices(handlerType);

        foreach (var handler in handlers)
        {
            if (handler == null) continue;
            
            var method = handlerType.GetMethod(nameof(IMessageHandler<IMessage>.HandleAsync));
            if (method != null)
            {
                await (Task)method.Invoke(handler, new object[] { context.Message, context.MessageContext, cancellationToken })!;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        
        if (_channel != null)
        {
            await _channel.CloseAsync();
            _channel.Dispose();
        }
        
        _concurrencySemaphore.Dispose();
        _stoppingCts.Dispose();
    }
}

