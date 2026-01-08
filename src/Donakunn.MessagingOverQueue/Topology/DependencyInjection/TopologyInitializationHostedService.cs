using Donakunn.MessagingOverQueue.DependencyInjection;
using Donakunn.MessagingOverQueue.Topology.Abstractions;
using Donakunn.MessagingOverQueues.Topology.Builders;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Donakunn.MessagingOverQueue.Topology.DependencyInjection;

/// <summary>
/// Hosted service that initializes topology on startup.
/// Responsible only for declaring topology to RabbitMQ - handler registration is done at configuration time.
/// </summary>
internal sealed class TopologyInitializationHostedService : IHostedService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<TopologyInitializationHostedService> _logger;
    private readonly TopologyReadySignal? _readySignal;

    public TopologyInitializationHostedService(
        IServiceProvider serviceProvider,
        ILogger<TopologyInitializationHostedService> logger,
        TopologyReadySignal? readySignal = null)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _readySignal = readySignal;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Initializing RabbitMQ topology");

        var registry = _serviceProvider.GetRequiredService<ITopologyRegistry>();
        var declarer = _serviceProvider.GetRequiredService<ITopologyDeclarer>();
        var configuration = _serviceProvider.GetService<TopologyConfiguration>();
        var discoveryResult = _serviceProvider.GetService<HandlerDiscoveryResult>();

        // Initialize handler invokers
        InitializeHandlerInvokers();

        // Register topologies from discovery result
        if (discoveryResult != null)
        {
            RegisterDiscoveredTopologies(registry, discoveryResult);
        }

        // Register manually configured topologies
        if (configuration != null)
        {
            RegisterManualTopologies(registry, configuration.Builder);
        }

        // Declare all registered topologies to RabbitMQ
        var topologies = registry.GetAllTopologies();

        if (topologies.Count > 0)
        {
            _logger.LogInformation("Declaring {Count} topologies on RabbitMQ broker", topologies.Count);
            await declarer.DeclareAllAsync(topologies, cancellationToken);
            _logger.LogInformation("Topology initialization completed successfully");
        }
        else
        {
            _logger.LogDebug("No topologies to declare");
        }

        // Signal that topology is ready for consumers to start
        _readySignal?.SetReady();
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Initializes handler invokers from registrations.
    /// </summary>
    private void InitializeHandlerInvokers()
    {
        var registry = _serviceProvider.GetRequiredService<Consuming.Handlers.IHandlerInvokerRegistry>();
        var factory = _serviceProvider.GetRequiredService<Consuming.Handlers.IHandlerInvokerFactory>();
        var registrations = _serviceProvider.GetServices<IHandlerInvokerRegistration>();

        foreach (var registration in registrations)
        {
            registration.Register(registry, factory);
        }

        _logger.LogDebug("Initialized {Count} handler invokers", registrations.Count());
    }

    /// <summary>
    /// Registers topologies from handler discovery.
    /// </summary>
    private void RegisterDiscoveredTopologies(ITopologyRegistry registry, HandlerDiscoveryResult discoveryResult)
    {
        foreach (var handlerRegistration in discoveryResult.Registrations)
        {
            if (handlerRegistration.TopologyDefinition != null)
            {
                registry.Register(handlerRegistration.TopologyDefinition);

                _logger.LogDebug(
                    "Registered topology for handler {Handler} on queue {Queue}",
                    handlerRegistration.HandlerType.Name,
                    handlerRegistration.QueueName);
            }
        }
    }

    /// <summary>
    /// Registers manually configured topologies.
    /// </summary>
    private void RegisterManualTopologies(ITopologyRegistry registry, TopologyBuilder builder)
    {
        foreach (var definition in builder.Definitions)
        {
            if (!string.IsNullOrEmpty(definition.Queue.Name))
            {
                registry.Register(definition);
                _logger.LogDebug("Registered manual topology for queue {Queue}", definition.Queue.Name);
            }
        }
    }
}
