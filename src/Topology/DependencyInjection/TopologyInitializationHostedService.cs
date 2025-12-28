using MessagingOverQueue.src.Abstractions.Consuming;
using MessagingOverQueue.src.Configuration.Options;
using MessagingOverQueue.src.Hosting;
using MessagingOverQueue.src.Topology.Abstractions;
using MessagingOverQueue.src.Topology.Builders;
using MessagingOverQueue.Topology.Conventions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessagingOverQueue.src.Topology.DependencyInjection;

/// <summary>
/// Hosted service that initializes topology on startup.
/// Supports handler-based auto-discovery for automatic consumer registration.
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
        var handlerRegistry = _serviceProvider.GetService<HandlerRegistryService>();

        if (configuration != null)
        {
            var builder = configuration.Builder;

            // Register manually configured topologies
            RegisterManualTopologies(registry, builder);

            // Process auto-discovery
            if (builder.AutoDiscoverEnabled && builder.AssembliesToScan.Count > 0)
            {
                if (builder.UseHandlerBasedDiscovery)
                {
                    ProcessHandlerBasedDiscovery(builder, scanner, registry, handlerRegistry);
                }
                else
                {
                    ProcessMessageTypeDiscovery(builder, scanner, provider, registry);
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

    private void RegisterManualTopologies(ITopologyRegistry registry, TopologyBuilder builder)
    {
        foreach (var definition in builder.Definitions)
        {
            if (!string.IsNullOrEmpty(definition.Queue.Name))
            {
                registry.Register(definition);
            }
        }
    }

    private void ProcessHandlerBasedDiscovery(
        TopologyBuilder builder,
        ITopologyScanner scanner,
        ITopologyRegistry registry,
        HandlerRegistryService? handlerRegistry)
    {
        _logger.LogDebug("Scanning {Count} assemblies for message handlers", builder.AssembliesToScan.Count);

        var handlerTopologies = scanner.ScanForHandlerTopology(builder.AssembliesToScan.ToArray());

        _logger.LogDebug("Found {Count} message handlers", handlerTopologies.Count);

        var namingConvention = new DefaultTopologyNamingConvention(builder.NamingOptions);
        var topologyBuilder = new HandlerTopologyBuilder(namingConvention, builder.ProviderOptions);

        foreach (var handlerInfo in handlerTopologies)
        {
            var registration = topologyBuilder.BuildHandlerRegistration(handlerInfo);
            
            // Register the topology
            if (registration.TopologyDefinition != null)
            {
                registry.Register(registration.TopologyDefinition);
            }

            // Register handler for DI and consumer setup
            handlerRegistry?.RegisterHandler(registration);

            _logger.LogDebug(
                "Registered handler {Handler} for message {Message} on queue {Queue}",
                handlerInfo.HandlerType.Name,
                handlerInfo.MessageType.Name,
                registration.QueueName);
        }
    }

    private void ProcessMessageTypeDiscovery(
        TopologyBuilder builder,
        ITopologyScanner scanner,
        ITopologyProvider provider,
        ITopologyRegistry registry)
    {
        _logger.LogDebug("Scanning {Count} assemblies for message types (legacy mode)", builder.AssembliesToScan.Count);

        var messageTypes = scanner.ScanForMessageTypes(builder.AssembliesToScan.ToArray());

        _logger.LogDebug("Found {Count} message types", messageTypes.Count);

        foreach (var messageTypeInfo in messageTypes)
        {
            var topology = provider.GetTopology(messageTypeInfo.MessageType);
            registry.Register(topology);
        }
    }

    private void ProcessMessageTopologyRegistrations(ITopologyRegistry registry, ITopologyProvider provider)
    {
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
                var namingOptions = _serviceProvider.GetService<TopologyConfiguration>()?.Builder.NamingOptions
                                   ?? new TopologyNamingOptions();

                var builderType = typeof(MessageTopologyBuilder<>).MakeGenericType(messageType);
                var builderInstance = Activator.CreateInstance(builderType,
                    System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance,
                    null,
                    [namingOptions],
                    null);

                if (builderInstance != null)
                {
                    var delegateType = typeof(Action<>).MakeGenericType(builderType);
                    var invokeMethod = delegateType.GetMethod("Invoke");
                    invokeMethod?.Invoke(configureAction, [builderInstance]);

                    var buildMethod = builderType.GetMethod("Build",
                        System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

                    if (buildMethod?.Invoke(builderInstance, null) is TopologyDefinition topology)
                    {
                        registry.Register(topology);
                    }
                }
            }
            else
            {
                var topology = provider.GetTopology(messageType);
                registry.Register(topology);
            }
        }
    }
}

/// <summary>
/// Service that holds discovered handler registrations for consumer setup.
/// </summary>
public sealed class HandlerRegistryService
{
    private readonly List<HandlerRegistration> _registrations = [];
    private readonly IServiceCollection _services;
    private readonly object _lock = new();

    public HandlerRegistryService(IServiceCollection services)
    {
        _services = services;
    }

    /// <summary>
    /// Registers a handler and sets up consumer registration.
    /// </summary>
    public void RegisterHandler(HandlerRegistration registration)
    {
        lock (_lock)
        {
            _registrations.Add(registration);

            // Register the handler in DI if not already registered
            var handlerInterfaceType = typeof(IMessageHandler<>).MakeGenericType(registration.MessageType);
            
            // Add consumer registration for this handler
            var consumerOptions = new ConsumerOptions
            {
                QueueName = registration.QueueName,
                PrefetchCount = registration.ConsumerConfig?.PrefetchCount ?? 10,
                MaxConcurrency = registration.ConsumerConfig?.MaxConcurrency ?? 1
            };

            _services.AddSingleton(new ConsumerRegistration 
            { 
                Options = consumerOptions,
                HandlerType = registration.HandlerType
            });
        }
    }

    /// <summary>
    /// Gets all registered handlers.
    /// </summary>
    public IReadOnlyList<HandlerRegistration> GetRegistrations()
    {
        lock (_lock)
        {
            return _registrations.AsReadOnly();
        }
    }
}
