using MessagingOverQueue.src.Abstractions.Consuming;
using MessagingOverQueue.src.Configuration.Options;
using MessagingOverQueue.src.Connection;
using MessagingOverQueue.src.Consuming.Handlers;
using MessagingOverQueue.src.Consuming.Middleware;
using MessagingOverQueue.src.Topology.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessagingOverQueue.src.Hosting;

/// <summary>
/// Hosted service that manages message consumers.
/// Single responsibility: start and stop consumers based on registered ConsumerRegistrations.
/// </summary>
public sealed class ConsumerHostedService : IHostedService, IAsyncDisposable
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IEnumerable<ConsumerRegistration> _registrations;
    private readonly ILogger<ConsumerHostedService> _logger;
    private readonly TopologyReadySignal? _topologyReadySignal;
    private readonly List<Consuming.RabbitMqConsumer> _consumers = [];
    private bool _disposed;

    public ConsumerHostedService(
        IServiceProvider serviceProvider,
        IEnumerable<ConsumerRegistration> registrations,
        ILogger<ConsumerHostedService> logger,
        TopologyReadySignal? topologyReadySignal = null)
    {
        _serviceProvider = serviceProvider;
        _registrations = registrations;
        _logger = logger;
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

        _logger.LogInformation("Starting consumer hosted service with {Count} consumers", registrationList.Count);

        foreach (var registration in registrationList)
        {
            try
            {
                var consumer = CreateConsumer(registration);
                _consumers.Add(consumer);

                await consumer.StartAsync(cancellationToken);
                _logger.LogInformation("Started consumer for queue '{Queue}'", registration.Options.QueueName);
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

    private Consuming.RabbitMqConsumer CreateConsumer(ConsumerRegistration registration)
    {
        var connectionPool = _serviceProvider.GetRequiredService<IRabbitMqConnectionPool>();
        var handlerInvokerRegistry = _serviceProvider.GetRequiredService<IHandlerInvokerRegistry>();
        var middlewares = _serviceProvider.GetServices<IConsumeMiddleware>();
        var consumerLogger = _serviceProvider.GetRequiredService<ILogger<Consuming.RabbitMqConsumer>>();

        return new Consuming.RabbitMqConsumer(
            connectionPool,
            _serviceProvider,
            handlerInvokerRegistry,
            registration.Options,
            middlewares,
            consumerLogger);
    }

    private async Task StopConsumerSafelyAsync(Consuming.RabbitMqConsumer consumer, CancellationToken cancellationToken)
    {
        try
        {
            await consumer.StopAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error stopping consumer");
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

