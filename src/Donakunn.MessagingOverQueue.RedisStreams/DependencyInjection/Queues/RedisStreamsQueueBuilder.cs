using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.Abstractions.Serialization;
using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Consuming.Handlers;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.DependencyInjection;
using Donakunn.MessagingOverQueue.Providers;
using Donakunn.MessagingOverQueue.Publishing.Middleware;
using Donakunn.MessagingOverQueue.RedisStreams.Configuration;
using Donakunn.MessagingOverQueue.RedisStreams.Connection;
using Donakunn.MessagingOverQueue.RedisStreams.HealthChecks;
using Donakunn.MessagingOverQueue.Resilience;
using Donakunn.MessagingOverQueue.Topology;
using Donakunn.MessagingOverQueue.Topology.Abstractions;
using Donakunn.MessagingOverQueue.Topology.Builders;
using Donakunn.MessagingOverQueue.Topology.Conventions;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection.Queues;

/// <summary>
/// Implementation of the Redis Streams queue builder.
/// </summary>
internal sealed class RedisStreamsQueueBuilder : IRedisStreamsQueueBuilder
{
    private readonly RedisStreamsOptionsBuilder _optionsBuilder = new();
    private bool _coreServicesRegistered;

    public IServiceCollection Services { get; }
    public IMessagingBuilder MessagingBuilder { get; }

    public RedisStreamsQueueBuilder(IServiceCollection services, IMessagingBuilder messagingBuilder)
    {
        Services = services;
        MessagingBuilder = messagingBuilder;
    }

    public IRedisStreamsQueueBuilder WithConnection(Action<RedisStreamsOptionsBuilder> configure)
    {
        configure(_optionsBuilder);
        EnsureCoreServicesRegistered();
        return this;
    }

    public IRedisStreamsQueueBuilder WithTopology(Action<TopologyBuilder> configure)
    {
        EnsureCoreServicesRegistered();
        MessagingBuilder.AddTopology(configure);

        // Register consumer hosted service after topology is configured
        // This must happen AFTER AddTopology to ensure proper startup ordering
        Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<Microsoft.Extensions.Hosting.IHostedService, Hosting.RedisStreamsConsumerHostedService>());

        return this;
    }

    public IRedisStreamsQueueBuilder WithHealthChecks(
        string name = "redis-streams",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
    {
        EnsureCoreServicesRegistered();

        Services.AddHealthChecks()
            .Add(new HealthCheckRegistration(
                name,
                sp => sp.GetRequiredService<RedisStreamsHealthCheck>(),
                failureStatus,
                tags));

        return this;
    }

    /// <summary>
    /// Ensures core Redis Streams services are registered.
    /// Called internally to set up the provider.
    /// </summary>
    internal void EnsureCoreServicesRegistered()
    {
        if (_coreServicesRegistered)
            return;

        _coreServicesRegistered = true;

        // Mark that a queue provider has been configured
        if (MessagingBuilder is MessagingBuilder mb)
        {
            mb.HasQueueProvider = true;
        }

        // Configure options using the builder
        Services.Configure<RedisStreamsOptions>(options => _optionsBuilder.Configure(options));

        // Core connection services
        Services.TryAddSingleton<IRedisConnectionPool, RedisConnectionPool>();

        // Register messaging provider abstraction for Redis Streams
        Services.TryAddSingleton<IMessagingProvider, RedisStreamsMessagingProvider>();

        // Redis Streams specific services
        Services.TryAddSingleton<RedisStreamsPublisher>();
        Services.TryAddSingleton<ITopologyDeclarer, RedisStreamsTopologyDeclarer>();

        // Register default topology services for standalone usage (without AddTopology)
        RegisterDefaultTopologyServices();

        // Register publisher interfaces - create a wrapper that uses the provider
        Services.TryAddSingleton<IMessagePublisher>(sp =>
            new RedisStreamsMessagePublisher(
                sp.GetRequiredService<RedisStreamsPublisher>(),
                sp.GetServices<IPublishMiddleware>(),
                sp.GetRequiredService<IMessageRoutingResolver>()));

        Services.TryAddSingleton<IEventPublisher>(sp =>
            (IEventPublisher)sp.GetRequiredService<IMessagePublisher>());

        Services.TryAddSingleton<ICommandSender>(sp =>
            (ICommandSender)sp.GetRequiredService<IMessagePublisher>());

        // Serialization services (shared with core)
        Services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();
        Services.TryAddSingleton<IMessageTypeResolver, MessageTypeResolver>();

        // Handler invoker infrastructure (populated by AddTopology)
        Services.TryAddSingleton<IHandlerInvokerRegistry, HandlerInvokerRegistry>();
        Services.TryAddSingleton<IHandlerInvokerFactory, HandlerInvokerFactory>();

        // Publish middleware (shared)
        Services.AddSingleton<IPublishMiddleware, LoggingMiddleware>();
        Services.AddSingleton<IPublishMiddleware, SerializationMiddleware>();

        // Consume middleware (shared - core logging and deserialization)
        Services.AddSingleton<IConsumeMiddleware, ConsumeLoggingMiddleware>();
        Services.AddSingleton<IConsumeMiddleware, DeserializationMiddleware>();

        // Resilience defaults (shared)
        Services.Configure<RetryOptions>(_ => { });
        Services.TryAddSingleton<IRetryPolicy, PollyRetryPolicy>();

        // Health check
        Services.TryAddSingleton<RedisStreamsHealthCheck>();

        // Hosted service for connection management
        Services.AddHostedService<RedisStreamsHostedService>();

        // Note: RedisStreamsConsumerHostedService is registered in WithTopology()
        // to ensure proper startup ordering
    }

    /// <summary>
    /// Registers default topology services for standalone usage without AddTopology.
    /// </summary>
    private void RegisterDefaultTopologyServices()
    {
        // Default topology registry for caching topology definitions
        Services.TryAddSingleton<ITopologyRegistry, TopologyRegistry>();

        // Default naming convention - only created if AddTopology hasn't registered one
        Services.TryAddSingleton<ITopologyNamingConvention>(_ =>
            new DefaultTopologyNamingConvention(new TopologyNamingOptions { ServiceName = "default" }));

        // Default topology provider using convention-based naming
        Services.TryAddSingleton<ITopologyProvider>(sp =>
            new ConventionBasedTopologyProvider(
                sp.GetRequiredService<ITopologyNamingConvention>(),
                sp.GetRequiredService<ITopologyRegistry>()));

        // Default message routing resolver
        Services.TryAddSingleton<IMessageRoutingResolver>(sp =>
            new MessageRoutingResolver(sp.GetRequiredService<ITopologyProvider>()));
    }
}
