using System.Reflection;
using AsyncronousComunication.DependencyInjection;
using AsyncronousComunication.Topology.Abstractions;
using AsyncronousComunication.Topology.Builders;
using AsyncronousComunication.Topology.Conventions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AsyncronousComunication.Topology.DependencyInjection;

/// <summary>
/// Extension methods for configuring topology services.
/// </summary>
public static class TopologyServiceCollectionExtensions
{
    /// <summary>
    /// Adds topology management services with auto-discovery.
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

        // Register the configuration action
        builder.Services.AddSingleton(new TopologyConfiguration
        {
            Builder = topologyBuilder
        });

        // Register hosted service to initialize topology
        builder.Services.AddHostedService<TopologyInitializationHostedService>();

        return builder;
    }

    /// <summary>
    /// Adds topology management with assembly scanning.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="assemblies">Assemblies to scan for message types.</param>
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
    /// <typeparam name="T">Type from the assembly to scan.</typeparam>
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
    /// Adds topology for specific message types.
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
