using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Consuming.Handlers;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.Hosting;
using Donakunn.MessagingOverQueue.RedisStreams.Configuration;
using Donakunn.MessagingOverQueue.RedisStreams.Connection;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Donakunn.MessagingOverQueue.RedisStreams.Hosting;

/// <summary>
/// Hosted service that manages Redis Streams message consumers.
/// Single responsibility: start and stop consumers based on registered ConsumerRegistrations.
/// </summary>
public sealed class RedisStreamsConsumerHostedService(
    IServiceProvider serviceProvider,
    IEnumerable<ConsumerRegistration> registrations,
    ILogger<RedisStreamsConsumerHostedService> logger,
    TopologyReadySignal? topologyReadySignal = null) : IHostedService, IAsyncDisposable
{
    private readonly List<RedisStreamsConsumer> _consumers = [];
    private bool _disposed;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var registrationList = registrations.ToList();

        if (registrationList.Count == 0)
        {
            logger.LogDebug("No consumers registered");
            return;
        }

        // Wait for topology to be declared before starting consumers
        if (topologyReadySignal != null)
        {
            logger.LogDebug("Waiting for topology initialization to complete");
            await topologyReadySignal.WaitAsync(cancellationToken);
            logger.LogDebug("Topology initialization completed, starting consumers");
        }

        logger.LogInformation("Starting Redis Streams consumer hosted service with {Count} consumers", registrationList.Count);

        var handlerInvokerRegistry = serviceProvider.GetRequiredService<IHandlerInvokerRegistry>();

        foreach (var registration in registrationList)
        {
            try
            {
                var consumer = CreateConsumer(registration);
                _consumers.Add(consumer);

                // Create handler that invokes the message pipeline
                var handler = CreateMessageHandler(handlerInvokerRegistry);
                await consumer.StartAsync(handler, cancellationToken);
                
                logger.LogInformation("Started consumer for stream '{StreamKey}'", consumer.SourceName);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to start consumer for queue '{Queue}'", registration.Options.QueueName);
                throw;
            }
        }

        logger.LogInformation("All consumers started ({Count} total)", _consumers.Count);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_consumers.Count == 0)
            return;

        logger.LogInformation("Stopping Redis Streams consumer hosted service");

        var stopTasks = _consumers.Select(c => StopConsumerSafelyAsync(c, cancellationToken));
        await Task.WhenAll(stopTasks);

        logger.LogInformation("All consumers stopped");
    }

    private RedisStreamsConsumer CreateConsumer(ConsumerRegistration registration)
    {
        var connectionPool = serviceProvider.GetRequiredService<IRedisConnectionPool>();
        var redisOptions = serviceProvider.GetRequiredService<IOptions<RedisStreamsOptions>>();
        var consumerLogger = serviceProvider.GetRequiredService<ILogger<RedisStreamsConsumer>>();

        // Build stream key from queue name (which follows topology conventions)
        var streamKey = BuildStreamKeyFromQueueName(registration.Options.QueueName, redisOptions.Value);
        var consumerGroup = registration.Options.QueueName; // Use queue name as consumer group

        return new RedisStreamsConsumer(
            connectionPool,
            redisOptions,
            registration.Options,
            streamKey,
            consumerGroup,
            consumerLogger);
    }

    private Func<ConsumeContext, CancellationToken, Task> CreateMessageHandler(
        IHandlerInvokerRegistry handlerInvokerRegistry)
    {
        return async (context, cancellationToken) =>
        {
            // Create a scope to resolve middlewares with scoped dependencies
            using var scope = serviceProvider.CreateScope();
            var scopedProvider = scope.ServiceProvider;
            var middlewares = scopedProvider.GetServices<IConsumeMiddleware>();
            
            var pipeline = new ConsumePipeline(middlewares, (ctx, ct) => HandleMessageAsync(ctx, ct, scopedProvider, handlerInvokerRegistry));
            await pipeline.ExecuteAsync(context, cancellationToken);
        };
    }

    private static async Task HandleMessageAsync(
        ConsumeContext context, 
        CancellationToken cancellationToken, 
        IServiceProvider scopedProvider,
        IHandlerInvokerRegistry handlerInvokerRegistry)
    {
        if (context.Message == null || context.MessageType == null)
        {
            return;
        }

        var invoker = handlerInvokerRegistry.GetInvoker(context.MessageType);
        if (invoker == null)
        {
            return;
        }

        await invoker.InvokeAsync(
            scopedProvider,
            context.Message,
            context.MessageContext,
            context.Data,
            cancellationToken);
    }

    private static string BuildStreamKeyFromQueueName(string queueName, RedisStreamsOptions options)
    {
        if (string.IsNullOrEmpty(options.StreamPrefix))
        {
            return queueName;
        }

        return $"{options.StreamPrefix}:{queueName}";
    }

    private async Task StopConsumerSafelyAsync(RedisStreamsConsumer consumer, CancellationToken cancellationToken)
    {
        try
        {
            await consumer.StopAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Error stopping consumer");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        foreach (var consumer in _consumers)
        {
            await consumer.DisposeAsync();
        }

        _consumers.Clear();
        _disposed = true;
    }
}
