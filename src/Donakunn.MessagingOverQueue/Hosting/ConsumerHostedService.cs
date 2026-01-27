using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Consuming.Handlers;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.Providers;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Donakunn.MessagingOverQueue.Hosting;

/// <summary>
/// Provider-agnostic hosted service that manages message consumers.
/// Uses <see cref="IMessagingProvider"/> to create consumers, making it work with any messaging backend.
/// </summary>
/// <remarks>
/// This service coordinates consumer lifecycle:
/// 1. Waits for topology initialization to complete (if signal is registered)
/// 2. Creates consumers via the registered messaging provider
/// 3. Starts all consumers with a message handler that invokes the middleware pipeline
/// 4. Gracefully stops consumers on shutdown
/// </remarks>
public sealed class ConsumerHostedService : IHostedService, IAsyncDisposable
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IEnumerable<ConsumerRegistration> _registrations;
    private readonly ILogger<ConsumerHostedService> _logger;
    private readonly TopologyReadySignal? _topologyReadySignal;
    private readonly List<IInternalConsumer> _consumers = [];
    private bool _disposed;

    public ConsumerHostedService(
        IServiceProvider serviceProvider,
        IEnumerable<ConsumerRegistration> registrations,
        ILogger<ConsumerHostedService> logger,
        TopologyReadySignal? topologyReadySignal = null)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _registrations = registrations ?? throw new ArgumentNullException(nameof(registrations));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _topologyReadySignal = topologyReadySignal;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var registrationList = _registrations.ToList();

        if (registrationList.Count == 0)
        {
            _logger.LogDebug("No consumers registered");
            return;
        }

        // Wait for topology to be declared before starting consumers
        if (_topologyReadySignal != null)
        {
            _logger.LogDebug("Waiting for topology initialization to complete");
            await _topologyReadySignal.WaitAsync(cancellationToken);
            _logger.LogDebug("Topology initialization completed, starting consumers");
        }

        var messagingProvider = _serviceProvider.GetRequiredService<IMessagingProvider>();
        var handlerInvokerRegistry = _serviceProvider.GetRequiredService<IHandlerInvokerRegistry>();

        _logger.LogInformation(
            "Starting consumer hosted service with {Count} consumers using {Provider} provider",
            registrationList.Count,
            messagingProvider.ProviderName);

        foreach (var registration in registrationList)
        {
            try
            {
                var consumer = await messagingProvider.CreateConsumerAsync(registration.Options, cancellationToken);
                _consumers.Add(consumer);

                // Create handler that invokes the message pipeline
                var handler = CreateMessageHandler(handlerInvokerRegistry);
                await consumer.StartAsync(handler, cancellationToken);

                _logger.LogInformation("Started consumer for '{Source}'", consumer.SourceName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start consumer for queue '{Queue}'", registration.Options.QueueName);
                throw;
            }
        }

        _logger.LogInformation("All consumers started ({Count} total)", _consumers.Count);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_consumers.Count == 0)
            return;

        _logger.LogInformation("Stopping consumer hosted service");

        var stopTasks = _consumers.Select(c => StopConsumerSafelyAsync(c, cancellationToken));
        await Task.WhenAll(stopTasks);

        _logger.LogInformation("All consumers stopped");
    }

    /// <summary>
    /// Creates a message handler that invokes the middleware pipeline and then the handler invoker.
    /// </summary>
    private Func<ConsumeContext, CancellationToken, Task> CreateMessageHandler(
        IHandlerInvokerRegistry handlerInvokerRegistry)
    {
        return async (context, cancellationToken) =>
        {
            // Create a scope to resolve middlewares with scoped dependencies
            using var scope = _serviceProvider.CreateScope();
            var scopedProvider = scope.ServiceProvider;
            var middlewares = scopedProvider.GetServices<IConsumeMiddleware>();

            var pipeline = new ConsumePipeline(
                middlewares,
                (ctx, ct) => InvokeHandlerAsync(ctx, ct, scopedProvider, handlerInvokerRegistry));

            await pipeline.ExecuteAsync(context, cancellationToken);
        };
    }

    /// <summary>
    /// Invokes the handler for a deserialized message.
    /// </summary>
    private static async Task InvokeHandlerAsync(
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

    private async Task StopConsumerSafelyAsync(IInternalConsumer consumer, CancellationToken cancellationToken)
    {
        try
        {
            await consumer.StopAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error stopping consumer '{Source}'", consumer.SourceName);
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

/// <summary>
/// Registration for a consumer, created during topology configuration.
/// </summary>
public sealed class ConsumerRegistration
{
    /// <summary>
    /// The consumer options (queue name, prefetch, concurrency).
    /// </summary>
    public ConsumerOptions Options { get; init; } = new();

    /// <summary>
    /// The handler type associated with this consumer (for diagnostics).
    /// </summary>
    public Type? HandlerType { get; init; }

    /// <summary>
    /// The exchange name for routing (used by some providers like Redis Streams).
    /// </summary>
    public string? ExchangeName { get; init; }

    /// <summary>
    /// The routing key for routing (used by some providers like Redis Streams).
    /// </summary>
    public string? RoutingKey { get; init; }
}
