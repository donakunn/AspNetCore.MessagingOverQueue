using MessagingOverQueue.src.Abstractions.Consuming;
using MessagingOverQueue.src.Abstractions.Serialization;
using MessagingOverQueue.src.Configuration.Options;
using MessagingOverQueue.src.Consuming.Handlers;
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
/// This is the single entry point for all handler scanning, consumer registration, and topology setup.
/// </summary>
public static class TopologyServiceCollectionExtensions
{
    /// <summary>
    /// Adds topology management services with handler-based auto-discovery.
    /// Scans assemblies for message handlers and automatically configures:
    /// - Handler DI registrations
    /// - Handler invokers (reflection-free dispatch)
    /// - Consumer registrations
    /// - RabbitMQ topology (exchanges, queues, bindings)
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

        var services = builder.Services;

        // Register core topology services
        RegisterTopologyServices(services, topologyBuilder);

        // Store configuration for hosted service
        services.AddSingleton(new TopologyConfiguration { Builder = topologyBuilder });

        // Register signal for coordinating topology initialization with consumer startup
        services.TryAddSingleton<TopologyReadySignal>();

        // Perform handler discovery and registration at configuration time
        if (topologyBuilder.AutoDiscoverEnabled && topologyBuilder.AssembliesToScan.Count > 0)
        {
            var discoveryResult = DiscoverAndRegisterHandlers(services, topologyBuilder);
            services.AddSingleton(discoveryResult);
        }
        else
        {
            services.AddSingleton(new HandlerDiscoveryResult());
        }

        // Register hosted service to initialize topology
        services.AddHostedService<TopologyInitializationHostedService>();

        // Register consumer hosted service if handlers were discovered
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<Microsoft.Extensions.Hosting.IHostedService, ConsumerHostedService>());

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
        return builder.AddTopology(topology => topology.ScanAssemblies(assemblies));
    }

    /// <summary>
    /// Adds topology management with assembly scanning from type markers.
    /// </summary>
    /// <typeparam name="T">Type from the assembly to scan for handlers.</typeparam>
    /// <param name="builder">The messaging builder.</param>
    /// <returns>The messaging builder for chaining.</returns>
    public static IMessagingBuilder AddTopologyFromAssemblyContaining<T>(this IMessagingBuilder builder)
    {
        return builder.AddTopology(topology => topology.ScanAssemblyContaining<T>());
    }

    /// <summary>
    /// Registers core topology services.
    /// </summary>
    private static void RegisterTopologyServices(IServiceCollection services, TopologyBuilder topologyBuilder)
    {
        // Register naming convention with configured options
        services.TryAddSingleton<ITopologyNamingConvention>(
            _ => new DefaultTopologyNamingConvention(topologyBuilder.NamingOptions));

        // Register topology registry (stores discovered topology definitions)
        services.TryAddSingleton<ITopologyRegistry, TopologyRegistry>();

        // Register topology scanner (discovers handlers and message types)
        services.TryAddSingleton<ITopologyScanner, TopologyScanner>();

        // Register topology provider (generates topology from conventions)
        services.TryAddSingleton<ITopologyProvider>(sp =>
        {
            var namingConvention = sp.GetRequiredService<ITopologyNamingConvention>();
            var registry = sp.GetRequiredService<ITopologyRegistry>();
            return new ConventionBasedTopologyProvider(namingConvention, registry, topologyBuilder.ProviderOptions);
        });

        // Register topology declarer (declares topology to RabbitMQ)
        services.TryAddSingleton<ITopologyDeclarer, TopologyDeclarer>();

        // Register routing resolver (determines routing for publishers)
        services.TryAddSingleton<IMessageRoutingResolver, MessageRoutingResolver>();
    }

    /// <summary>
    /// Discovers handlers from assemblies and registers all required services.
    /// This is the unified registration point - no duplicate scanning elsewhere.
    /// </summary>
    private static HandlerDiscoveryResult DiscoverAndRegisterHandlers(
        IServiceCollection services,
        TopologyBuilder topologyBuilder)
    {
        var scanner = new TopologyScanner();
        var namingConvention = new DefaultTopologyNamingConvention(topologyBuilder.NamingOptions);
        var handlerTopologyBuilder = new HandlerTopologyBuilder(namingConvention, topologyBuilder.ProviderOptions);

        var assemblies = topologyBuilder.AssembliesToScan.ToArray();
        var handlerTopologies = scanner.ScanForHandlerTopology(assemblies);

        var result = new HandlerDiscoveryResult();
        var registeredQueues = new HashSet<string>();

        foreach (var handlerInfo in handlerTopologies)
        {
            var registration = handlerTopologyBuilder.BuildHandlerRegistration(handlerInfo);

            // 1. Register handler in DI
            var handlerInterfaceType = typeof(IMessageHandler<>).MakeGenericType(handlerInfo.MessageType);
            services.AddScoped(handlerInterfaceType, handlerInfo.HandlerType);

            // 2. Register handler invoker for reflection-free dispatch
            RegisterHandlerInvoker(services, handlerInfo.MessageType);

            // 3. Register message type for serialization
            RegisterMessageType(services, handlerInfo.MessageType);

            // 4. Register consumer for this queue (avoid duplicates for shared queues)
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

            // 5. Store handler registration for topology initialization
            result.AddRegistration(registration);
        }

        return result;
    }

    /// <summary>
    /// Registers a handler invoker for a message type.
    /// </summary>
    private static void RegisterHandlerInvoker(IServiceCollection services, Type messageType)
    {
        var registrationType = typeof(HandlerInvokerRegistration<>).MakeGenericType(messageType);
        var registration = (IHandlerInvokerRegistration)Activator.CreateInstance(registrationType)!;
        services.AddSingleton(registration);
    }

    /// <summary>
    /// Registers a message type for serialization/deserialization.
    /// </summary>
    private static void RegisterMessageType(IServiceCollection services, Type messageType)
    {
        services.AddSingleton<IMessageTypeRegistration>(new MessageTypeRegistration(messageType));
    }
}

/// <summary>
/// Configuration holding the topology builder settings.
/// </summary>
internal sealed class TopologyConfiguration
{
    public TopologyBuilder Builder { get; init; } = null!;
}

/// <summary>
/// Results from handler discovery, used by TopologyInitializationHostedService.
/// </summary>
public sealed class HandlerDiscoveryResult
{
    private readonly List<HandlerRegistration> _registrations = [];

    public IReadOnlyList<HandlerRegistration> Registrations => _registrations;

    internal void AddRegistration(HandlerRegistration registration)
    {
        _registrations.Add(registration);
    }
}

/// <summary>
/// Interface for message type registrations used by serialization.
/// </summary>
public interface IMessageTypeRegistration
{
    /// <summary>
    /// The message type.
    /// </summary>
    Type MessageType { get; }

    /// <summary>
    /// Registers the message type with the resolver.
    /// </summary>
    void Register(IMessageTypeResolver resolver);
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

    public void Register(IMessageTypeResolver resolver)
    {
        resolver.RegisterType(MessageType);
    }
}

/// <summary>
/// Signal to coordinate topology initialization with consumer startup.
/// </summary>
public sealed class TopologyReadySignal
{
    private readonly TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Signals that topology has been declared and is ready.
    /// </summary>
    public void SetReady() => _tcs.TrySetResult();

    /// <summary>
    /// Waits until topology is ready.
    /// </summary>
    public Task WaitAsync(CancellationToken cancellationToken = default)
    {
        return _tcs.Task.WaitAsync(cancellationToken);
    }
}
