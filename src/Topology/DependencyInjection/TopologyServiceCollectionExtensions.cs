using MessagingOverQueue.src.Abstractions.Consuming;
using MessagingOverQueue.src.Configuration.Options;
using MessagingOverQueue.src.DependencyInjection;
using MessagingOverQueue.src.Hosting;
using MessagingOverQueue.src.Topology.Abstractions;
using MessagingOverQueue.src.Topology.Builders;
using MessagingOverQueue.Topology.Conventions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System.Reflection;

namespace MessagingOverQueue.src.Topology.DependencyInjection;

/// <summary>
/// Extension methods for configuring topology services with handler-based auto-discovery.
/// </summary>
public static class TopologyServiceCollectionExtensions
{
    /// <summary>
    /// Adds topology management services with handler-based auto-discovery.
    /// Scans assemblies for message handlers and automatically configures topology and consumers.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="configure">Action to configure topology.</param>
    /// <returns>The messaging builder for chaining.</returns>
    public static IMessagingBuilder AddTopology(
        this IMessagingBuilder builder,
        Action<TopologyBuilder>? configure = null)
    {
        var topologyBuilder = new TopologyBuilder();
        configure?.Invoke(topologyBuilder);

        // Register naming convention
        builder.Services.TryAddSingleton<ITopologyNamingConvention>(sp =>
            new DefaultTopologyNamingConvention(topologyBuilder.NamingOptions));

        // Register topology registry
        builder.Services.TryAddSingleton<ITopologyRegistry, TopologyRegistry>();

        // Register topology scanner
        builder.Services.TryAddSingleton<ITopologyScanner, TopologyScanner>();

        // Register topology provider
        builder.Services.TryAddSingleton<ITopologyProvider>(sp =>
        {
            var namingConvention = sp.GetRequiredService<ITopologyNamingConvention>();
            var registry = sp.GetRequiredService<ITopologyRegistry>();
            return new ConventionBasedTopologyProvider(namingConvention, registry, topologyBuilder.ProviderOptions);
        });

        // Register topology declarer
        builder.Services.TryAddSingleton<ITopologyDeclarer, TopologyDeclarer>();

        // Register routing resolver
        builder.Services.TryAddSingleton<IMessageRoutingResolver, MessageRoutingResolver>();

        // Register the configuration
        builder.Services.AddSingleton(new TopologyConfiguration
        {
            Builder = topologyBuilder
        });

        // Register hosted service to initialize topology
        builder.Services.AddHostedService<TopologyInitializationHostedService>();

        // If handler-based discovery is enabled, register handlers and consumers
        if (topologyBuilder.UseHandlerBasedDiscovery && topologyBuilder.AutoDiscoverEnabled)
        {
            RegisterHandlersAndConsumers(builder.Services, topologyBuilder);
        }

        return builder;
    }

    /// <summary>
    /// Adds topology management with assembly scanning for handlers.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="assemblies">Assemblies to scan for message handlers.</param>
    /// <returns>The messaging builder for chaining.</returns>
    public static IMessagingBuilder AddTopologyFromAssemblies(
        this IMessagingBuilder builder,
        params Assembly[] assemblies)
    {
        return builder.AddTopology(topology =>
        {
            topology.ScanAssemblies(assemblies);
        });
    }

    /// <summary>
    /// Adds topology management with assembly scanning from type markers.
    /// </summary>
    /// <typeparam name="T">Type from the assembly to scan for handlers.</typeparam>
    /// <param name="builder">The messaging builder.</param>
    /// <returns>The messaging builder for chaining.</returns>
    public static IMessagingBuilder AddTopologyFromAssemblyContaining<T>(
        this IMessagingBuilder builder)
    {
        return builder.AddTopology(topology =>
        {
            topology.ScanAssemblyContaining<T>();
        });
    }

    /// <summary>
    /// Adds topology for specific message types (manual configuration).
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="configure">Optional topology configuration.</param>
    /// <returns>The messaging builder for chaining.</returns>
    public static IMessagingBuilder AddTopologyFor<TMessage>(
        this IMessagingBuilder builder,
        Action<MessageTopologyBuilder<TMessage>>? configure = null)
    {
        builder.Services.AddSingleton(new MessageTopologyRegistration<TMessage>
        {
            Configure = configure
        });

        return builder;
    }

    /// <summary>
    /// Registers handlers and consumers discovered from assembly scanning.
    /// </summary>
    private static void RegisterHandlersAndConsumers(IServiceCollection services, TopologyBuilder topologyBuilder)
    {
        if (topologyBuilder.AssembliesToScan.Count == 0)
            return;

        var scanner = new TopologyScanner();
        var handlerTopologies = scanner.ScanForHandlerTopology(topologyBuilder.AssembliesToScan.ToArray());

        var namingConvention = new DefaultTopologyNamingConvention(topologyBuilder.NamingOptions);
        var handlerTopologyBuilder = new HandlerTopologyBuilder(namingConvention, topologyBuilder.ProviderOptions);

        var registeredQueues = new HashSet<string>();

        foreach (var handlerInfo in handlerTopologies)
        {
            var registration = handlerTopologyBuilder.BuildHandlerRegistration(handlerInfo);

            // Register handler in DI
            var handlerInterfaceType = typeof(IMessageHandler<>).MakeGenericType(handlerInfo.MessageType);
            services.AddScoped(handlerInterfaceType, handlerInfo.HandlerType);

            // Register message type for serialization
            services.AddSingleton<IMessageTypeRegistration>(
                new MessageTypeRegistration(handlerInfo.MessageType));

            // Register consumer for this queue (avoid duplicates)
            if (!registeredQueues.Contains(registration.QueueName))
            {
                registeredQueues.Add(registration.QueueName);

                var consumerOptions = new ConsumerOptions
                {
                    QueueName = registration.QueueName,
                    PrefetchCount = registration.ConsumerConfig?.PrefetchCount ?? 10,
                    MaxConcurrency = registration.ConsumerConfig?.MaxConcurrency ?? 1
                };

                services.AddSingleton(new ConsumerRegistration
                {
                    Options = consumerOptions,
                    HandlerType = handlerInfo.HandlerType
                });
            }

            // Store handler registration for topology builder
            topologyBuilder.AddHandlerRegistration(registration);
        }

        // Ensure consumer hosted service is registered
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<Microsoft.Extensions.Hosting.IHostedService, ConsumerHostedService>());
    }
}

/// <summary>
/// Internal class to hold topology configuration.
/// </summary>
internal sealed class TopologyConfiguration
{
    public TopologyBuilder Builder { get; init; } = null!;
}

/// <summary>
/// Internal class for message-specific topology registration.
/// </summary>
internal sealed class MessageTopologyRegistration<TMessage>
{
    public Action<MessageTopologyBuilder<TMessage>>? Configure { get; init; }
}

/// <summary>
/// Interface for message type registrations used by serialization.
/// </summary>
internal interface IMessageTypeRegistration
{
    Type MessageType { get; }
}

/// <summary>
/// Registration for a message type.
/// </summary>
internal sealed class MessageTypeRegistration : IMessageTypeRegistration
{
    public Type MessageType { get; }

    public MessageTypeRegistration(Type messageType)
    {
        MessageType = messageType;
    }
}
