using Donakunn.MessagingOverQueue.DependencyInjection;
using Donakunn.MessagingOverQueue.Topology.Abstractions;
using Donakunn.MessagingOverQueue.Topology.Builders;
using Donakunn.MessagingOverQueue.Abstractions.Serialization;
using Donakunn.MessagingOverQueue.Persistence.Providers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Donakunn.MessagingOverQueue.Topology.DependencyInjection;

/// <summary>
/// Hosted service that initializes topology on startup.
/// Responsible only for declaring topology to the message broker - handler registration is done at configuration time.
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
        _logger.LogInformation("Initializing messaging topology");

        var registry = _serviceProvider.GetRequiredService<ITopologyRegistry>();
        var declarer = _serviceProvider.GetRequiredService<ITopologyDeclarer>();
        var configuration = _serviceProvider.GetService<TopologyConfiguration>();
        var discoveryResult = _serviceProvider.GetService<HandlerDiscoveryResult>();

        // Initialize handler invokers
        InitializeHandlerInvokers();

        // Initialize message type registrations for deserialization
        InitializeMessageTypeRegistrations();

        // Initialize message store schema if provider is registered
        await InitializeMessageStoreSchemaAsync(cancellationToken);

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

        // Declare all registered topologies to message broker
        var topologies = registry.GetAllTopologies();

        if (topologies.Count > 0)
        {
            _logger.LogInformation("Declaring {Count} topologies on message broker", topologies.Count);
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
    /// Initializes message type registrations for deserialization.
    /// </summary>
    private void InitializeMessageTypeRegistrations()
    {
        var resolver = _serviceProvider.GetRequiredService<IMessageTypeResolver>();
        var registrations = _serviceProvider.GetServices<IMessageTypeRegistration>();

        foreach (var registration in registrations)
        {
            registration.Register(resolver);
        }

        _logger.LogDebug("Initialized {Count} message type registrations", registrations.Count());
    }

    /// <summary>
    /// Initializes the message store schema if a provider is registered.
    /// This ensures the schema exists before consumers start processing messages.
    /// </summary>
    private async Task InitializeMessageStoreSchemaAsync(CancellationToken cancellationToken)
    {
        var provider = _serviceProvider.GetService<IMessageStoreProvider>();
        if (provider == null)
        {
            _logger.LogDebug("No message store provider registered, skipping schema initialization");
            return;
        }

        try
        {
            await provider.EnsureSchemaAsync(cancellationToken);
            _logger.LogDebug("Message store schema initialized");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize message store schema");
            throw;
        }
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
