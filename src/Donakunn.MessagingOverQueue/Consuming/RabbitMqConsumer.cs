using Donakunn.MessagingOverQueue.Abstractions.Consuming;
using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Connection;
using Donakunn.MessagingOverQueue.Consuming.Handlers;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Donakunn.MessagingOverQueue.Consuming;

/// <summary>
/// RabbitMQ implementation of message consumer.
/// </summary>
public class RabbitMqConsumer(
    IRabbitMqConnectionPool connectionPool,
    IServiceProvider serviceProvider,
    IHandlerInvokerRegistry handlerInvokerRegistry,
    ConsumerOptions options,
    ILogger<RabbitMqConsumer> logger) : IMessageConsumer
{
    private IChannel? _channel;
    private string? _consumerTag;
    private readonly SemaphoreSlim _concurrencySemaphore = new(options.MaxConcurrency, options.MaxConcurrency);
    private volatile bool _isRunning;
    private readonly CancellationTokenSource _stoppingCts = new();

    public bool IsRunning => _isRunning;

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_isRunning)
            return;

        logger.LogInformation("Starting consumer for queue '{Queue}' with prefetch {Prefetch}",
            options.QueueName, options.PrefetchCount);

        _channel = await connectionPool.CreateDedicatedChannelAsync(cancellationToken);
        await _channel.BasicQosAsync(0, options.PrefetchCount, false, cancellationToken);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += OnMessageReceivedAsync;

        _consumerTag = await _channel.BasicConsumeAsync(
            queue: options.QueueName,
            autoAck: options.AutoAck,
            consumerTag: options.ConsumerTag ?? $"consumer-{Guid.NewGuid():N}",
            consumer: consumer,
            cancellationToken: cancellationToken);

        _isRunning = true;
        logger.LogInformation("Consumer started with tag '{ConsumerTag}'", _consumerTag);
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (!_isRunning)
            return;

        logger.LogInformation("Stopping consumer '{ConsumerTag}'", _consumerTag);

        await _stoppingCts.CancelAsync();

        if (_channel != null && _consumerTag != null)
        {
            try
            {
                await _channel.BasicCancelAsync(_consumerTag, cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Error canceling consumer");
            }
        }

        _isRunning = false;
        logger.LogInformation("Consumer stopped");
    }

    private async Task OnMessageReceivedAsync(object sender, BasicDeliverEventArgs args)
    {
        await _concurrencySemaphore.WaitAsync(_stoppingCts.Token);

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_stoppingCts.Token);
            cts.CancelAfter(options.ProcessingTimeout);

            await ProcessMessageAsync(args, cts.Token);
        }
        catch (OperationCanceledException) when (_stoppingCts.IsCancellationRequested)
        {
            logger.LogDebug("Message processing cancelled due to shutdown");
            if (!options.AutoAck && _channel != null)
            {
                await _channel.BasicNackAsync(args.DeliveryTag, false, true);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing message, delivery tag: {DeliveryTag}", args.DeliveryTag);
            if (!options.AutoAck && _channel != null)
            {
                await _channel.BasicNackAsync(args.DeliveryTag, false, options.RequeueOnFailure);
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
            queueName: options.QueueName,
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

        // Create a scope to resolve middlewares with scoped dependencies
        using var scope = serviceProvider.CreateScope();
        var middlewares = scope.ServiceProvider.GetServices<IConsumeMiddleware>();
        
        var pipeline = new ConsumePipeline(middlewares, (ctx, ct) => HandleMessageAsync(ctx, ct, scope.ServiceProvider));
        await pipeline.ExecuteAsync(context, cancellationToken);

        if (!options.AutoAck && _channel != null)
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

    private async Task HandleMessageAsync(ConsumeContext context, CancellationToken cancellationToken, IServiceProvider scopedProvider)
    {
        if (context.Message == null || context.MessageType == null)
        {
            logger.LogWarning("Message was not deserialized, skipping handler invocation");
            return;
        }

        var invoker = handlerInvokerRegistry.GetInvoker(context.MessageType);
        if (invoker == null)
        {
            logger.LogWarning(
                "No handler invoker registered for message type {MessageType}",
                context.MessageType.Name);
            return;
        }

        await invoker.InvokeAsync(
            scopedProvider,
            context.Message,
            context.MessageContext,
            context.Data,
            cancellationToken);
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

