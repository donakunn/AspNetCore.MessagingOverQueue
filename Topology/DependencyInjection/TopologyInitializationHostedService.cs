using AsyncronousComunication.Topology.Abstractions;
using AsyncronousComunication.Topology.Builders;
using AsyncronousComunication.Topology.Conventions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AsyncronousComunication.Topology.DependencyInjection;

/// <summary>
/// Hosted service that initializes topology on startup.
/// </summary>
internal sealed class TopologyInitializationHostedService : IHostedService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<TopologyInitializationHostedService> _logger;

    public TopologyInitializationHostedService(
        IServiceProvider serviceProvider,
        ILogger<TopologyInitializationHostedService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Initializing RabbitMQ topology");

        var registry = _serviceProvider.GetRequiredService<ITopologyRegistry>();
        var scanner = _serviceProvider.GetRequiredService<ITopologyScanner>();
        var provider = _serviceProvider.GetRequiredService<ITopologyProvider>();
        var declarer = _serviceProvider.GetRequiredService<ITopologyDeclarer>();
        var configuration = _serviceProvider.GetService<TopologyConfiguration>();

        // Process topology builder configuration
        if (configuration != null)
        {
            var builder = configuration.Builder;

            // Register manually configured topologies
            foreach (var definition in builder.Definitions)
            {
                if (!string.IsNullOrEmpty(definition.Queue.Name))
                {
                    registry.Register(definition);
                }
            }

            // Scan assemblies if auto-discovery is enabled
            if (builder.AutoDiscoverEnabled && builder.AssembliesToScan.Count > 0)
            {
                _logger.LogDebug("Scanning {Count} assemblies for message types", builder.AssembliesToScan.Count);
                
                var messageTypes = scanner.ScanForMessageTypes(builder.AssembliesToScan.ToArray());
                
                _logger.LogDebug("Found {Count} message types", messageTypes.Count);

                foreach (var messageTypeInfo in messageTypes)
                {
                    // Get topology from provider (uses conventions + attributes)
                    var topology = provider.GetTopology(messageTypeInfo.MessageType);
                    registry.Register(topology);
                }
            }
        }

        // Process individual message topology registrations
        ProcessMessageTopologyRegistrations(registry, provider);

        // Declare all registered topologies
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
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    private void ProcessMessageTopologyRegistrations(ITopologyRegistry registry, ITopologyProvider provider)
    {
        // Get all registered message topology registrations using reflection
        // This allows for type-specific configuration
        var registrations = _serviceProvider.GetServices<object>()
            .Where(s => s.GetType().IsGenericType && 
                       s.GetType().GetGenericTypeDefinition() == typeof(MessageTopologyRegistration<>));

        foreach (var registration in registrations)
        {
            var registrationType = registration.GetType();
            var messageType = registrationType.GetGenericArguments()[0];
            
            var configureProperty = registrationType.GetProperty("Configure");
            var configureAction = configureProperty?.GetValue(registration);

            if (configureAction != null)
            {
                // Build topology with custom configuration
                var namingOptions = _serviceProvider.GetService<TopologyConfiguration>()?.Builder.NamingOptions 
                                   ?? new TopologyNamingOptions();
                
                // Use reflection to create and configure the builder
                var builderType = typeof(MessageTopologyBuilder<>).MakeGenericType(messageType);
                var builderInstance = Activator.CreateInstance(builderType, 
                    System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, 
                    null, 
                    new object[] { namingOptions }, 
                    null);

                if (builderInstance != null)
                {
                    // Invoke the configure action
                    var delegateType = typeof(Action<>).MakeGenericType(builderType);
                    var invokeMethod = delegateType.GetMethod("Invoke");
                    invokeMethod?.Invoke(configureAction, new[] { builderInstance });

                    // Build and register
                    var buildMethod = builderType.GetMethod("Build", 
                        System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                    var topology = buildMethod?.Invoke(builderInstance, null) as TopologyDefinition;
                    
                    if (topology != null)
                    {
                        registry.Register(topology);
                    }
                }
            }
            else
            {
                // Use default topology from provider
                var topology = provider.GetTopology(messageType);
                registry.Register(topology);
            }
        }
    }
}
