using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Connection;
using Donakunn.MessagingOverQueue.Consuming.Handlers;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;
using MessagingOverQueue.src.Consuming;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Donakunn.MessagingOverQueue.Hosting;

/// <summary>
/// Hosted service that manages message consumers.
/// Single responsibility: start and stop consumers based on registered ConsumerRegistrations.
/// </summary>
public sealed class ConsumerHostedService(
    IServiceProvider serviceProvider,
    IEnumerable<ConsumerRegistration> registrations,
    ILogger<ConsumerHostedService> logger,
    TopologyReadySignal? topologyReadySignal = null) : IHostedService, IAsyncDisposable
{
    private readonly List<RabbitMqConsumer> _consumers = [];
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

        logger.LogInformation("Starting consumer hosted service with {Count} consumers", registrationList.Count);

        foreach (var registration in registrationList)
        {
            try
            {
                var consumer = CreateConsumer(registration);
                _consumers.Add(consumer);

                await consumer.StartAsync(cancellationToken);
                logger.LogInformation("Started consumer for queue '{Queue}'", registration.Options.QueueName);
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

        logger.LogInformation("Stopping consumer hosted service");

        var stopTasks = _consumers.Select(c => StopConsumerSafelyAsync(c, cancellationToken));
        await Task.WhenAll(stopTasks);

        logger.LogInformation("All consumers stopped");
    }

    private RabbitMqConsumer CreateConsumer(ConsumerRegistration registration)
    {
        var connectionPool = serviceProvider.GetRequiredService<IRabbitMqConnectionPool>();
        var handlerInvokerRegistry = serviceProvider.GetRequiredService<IHandlerInvokerRegistry>();
        var middlewares = serviceProvider.GetServices<IConsumeMiddleware>();
        var consumerLogger = serviceProvider.GetRequiredService<ILogger<RabbitMqConsumer>>();

        return new RabbitMqConsumer(
            connectionPool,
            serviceProvider,
            handlerInvokerRegistry,
            registration.Options,
            middlewares,
            consumerLogger);
    }

    private async Task StopConsumerSafelyAsync(RabbitMqConsumer consumer, CancellationToken cancellationToken)
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
}

