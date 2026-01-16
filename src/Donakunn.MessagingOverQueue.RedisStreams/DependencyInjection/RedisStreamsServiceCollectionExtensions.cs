using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.Abstractions.Serialization;
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
using Donakunn.MessagingOverQueue.Topology.Conventions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection;

/// <summary>
/// Extension methods for registering Redis Streams messaging services.
/// </summary>
public static class RedisStreamsServiceCollectionExtensions
{
    /// <summary>
    /// Adds Redis Streams messaging services using fluent configuration.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configuration action for Redis Streams options.</param>
    /// <returns>The messaging builder for further configuration.</returns>
    /// <example>
    /// <code>
    /// services.AddRedisStreamsMessaging(options => options
    ///     .UseConnectionString("localhost:6379")
    ///     .WithStreamPrefix("myapp")
    ///     .ConfigureConsumer(batchSize: 20)
    ///     .WithTimeBasedRetention(TimeSpan.FromDays(7)));
    /// </code>
    /// </example>
    public static IMessagingBuilder AddRedisStreamsMessaging(
        this IServiceCollection services,
        Action<RedisStreamsOptionsBuilder> configure)
    {
        var builder = new RedisStreamsOptionsBuilder();

        configure(builder);

        services.Configure<RedisStreamsOptions>(options => builder.Configure(options));

        return services.AddRedisStreamsMessagingCore();
    }

    /// <summary>
    /// Adds Redis Streams messaging services using IConfiguration (appsettings.json).
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The configuration instance.</param>
    /// <param name="sectionName">The configuration section name (default: "RedisStreams").</param>
    /// <returns>The messaging builder for further configuration.</returns>
    /// <example>
    /// <code>
    /// // appsettings.json:
    /// // {
    /// //   "RedisStreams": {
    /// //     "ConnectionString": "localhost:6379",
    /// //     "StreamPrefix": "myapp"
    /// //   }
    /// // }
    /// 
    /// services.AddRedisStreamsMessaging(configuration);
    /// </code>
    /// </example>
    public static IMessagingBuilder AddRedisStreamsMessaging(
        this IServiceCollection services,
        IConfiguration configuration,
        string? sectionName = null)
    {
        var section = configuration.GetSection(sectionName ?? RedisStreamsOptions.SectionName);
        services.Configure<RedisStreamsOptions>(section);

        return services.AddRedisStreamsMessagingCore();
    }

    /// <summary>
    /// Adds Redis Streams messaging services using IConfiguration and fluent configuration.
    /// Fluent configuration takes precedence over appsettings.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The configuration instance.</param>
    /// <param name="configure">Additional fluent configuration.</param>
    /// <param name="sectionName">The configuration section name (default: "RedisStreams").</param>
    /// <returns>The messaging builder for further configuration.</returns>
    public static IMessagingBuilder AddRedisStreamsMessaging(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<RedisStreamsOptionsBuilder> configure,
        string? sectionName = null)
    {
        var section = configuration.GetSection(sectionName ?? RedisStreamsOptions.SectionName);
        services.Configure<RedisStreamsOptions>(section);

        var builder = new RedisStreamsOptionsBuilder();
        configure(builder);
        services.PostConfigure<RedisStreamsOptions>(options => builder.Configure(options));

        return services.AddRedisStreamsMessagingCore();
    }

    /// <summary>
    /// Adds Redis Streams messaging with default configuration (localhost:6379).
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The messaging builder for further configuration.</returns>
    public static IMessagingBuilder AddRedisStreamsMessaging(this IServiceCollection services)
    {
        services.Configure<RedisStreamsOptions>(_ => { });
        return services.AddRedisStreamsMessagingCore();
    }

    /// <summary>
    /// Core method that registers all Redis Streams services.
    /// </summary>
    private static IMessagingBuilder AddRedisStreamsMessagingCore(this IServiceCollection services)
    {
        // Core connection services
        services.TryAddSingleton<IRedisConnectionPool, RedisConnectionPool>();

        // Register messaging provider abstraction for Redis Streams
        services.TryAddSingleton<IMessagingProvider, RedisStreamsMessagingProvider>();

        // Redis Streams specific services
        services.TryAddSingleton<RedisStreamsPublisher>();
        services.TryAddSingleton<ITopologyDeclarer, RedisStreamsTopologyDeclarer>();

        // Register default topology services for standalone usage (without AddTopology)
        // These use TryAddSingleton so AddTopology can override them when called
        RegisterDefaultTopologyServices(services);

        // Register publisher interfaces - create a wrapper that uses the provider
        services.TryAddSingleton<IMessagePublisher>(sp =>
            new RedisStreamsMessagePublisher(
                sp.GetRequiredService<RedisStreamsPublisher>(),
                sp.GetServices<IPublishMiddleware>(),
                sp.GetRequiredService<IMessageRoutingResolver>()));

        services.TryAddSingleton<IEventPublisher>(sp =>
            (IEventPublisher)sp.GetRequiredService<IMessagePublisher>());

        services.TryAddSingleton<ICommandSender>(sp =>
            (ICommandSender)sp.GetRequiredService<IMessagePublisher>());

        // Serialization services (shared with core)
        services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.TryAddSingleton<IMessageTypeResolver, MessageTypeResolver>();

        // Handler invoker infrastructure (populated by AddTopology)
        services.TryAddSingleton<IHandlerInvokerRegistry, HandlerInvokerRegistry>();
        services.TryAddSingleton<IHandlerInvokerFactory, HandlerInvokerFactory>();

        // Publish middleware (shared)
        services.AddSingleton<IPublishMiddleware, LoggingMiddleware>();
        services.AddSingleton<IPublishMiddleware, SerializationMiddleware>();

        // Consume middleware (shared)
        services.AddSingleton<IConsumeMiddleware, ConsumeLoggingMiddleware>();
        services.AddSingleton<IConsumeMiddleware, DeserializationMiddleware>();

        // Resilience (shared)
        services.Configure<Donakunn.MessagingOverQueue.Configuration.Options.RetryOptions>(_ => { });
        services.TryAddSingleton<IRetryPolicy, PollyRetryPolicy>();

        // Health check
        services.TryAddSingleton<RedisStreamsHealthCheck>();

        // Hosted service for connection management
        services.AddHostedService<RedisStreamsHostedService>();

        // Note: RedisStreamsConsumerHostedService is NOT registered here.
        // It must be registered AFTER TopologyInitializationHostedService (from AddTopology)
        // to avoid deadlock. The consumer service waits for TopologyReadySignal, which is
        // set by TopologyInitializationHostedService. If the consumer is registered first,
        // it blocks host startup and prevents topology initialization from ever running.
        // Registration is deferred to AddRedisStreamsConsumerHostedService extension method
        // which should be called after AddTopology.

        return new MessagingBuilder(services);
    }

    /// <summary>
    /// Registers default topology services for standalone usage without AddTopology.
    /// These services are used only when AddTopology is NOT called.
    /// Note: TopologyNamingOptions and ITopologyNamingConvention are NOT registered here
    /// to avoid conflicts with AddTopology configuration. They are lazily created
    /// with defaults only when needed and AddTopology hasn't been called.
    /// </summary>
    private static void RegisterDefaultTopologyServices(IServiceCollection services)
    {
        // Default topology registry for caching topology definitions
        services.TryAddSingleton<ITopologyRegistry, TopologyRegistry>();

        // Default naming convention - only created if AddTopology hasn't registered one
        // Uses lazy initialization with default options to avoid registration conflicts
        services.TryAddSingleton<ITopologyNamingConvention>(_ =>
            new DefaultTopologyNamingConvention(new TopologyNamingOptions { ServiceName = "default" }));

        // Default topology provider using convention-based naming
        services.TryAddSingleton<ITopologyProvider>(sp =>
            new ConventionBasedTopologyProvider(
                sp.GetRequiredService<ITopologyNamingConvention>(),
                sp.GetRequiredService<ITopologyRegistry>()));

        // Default message routing resolver
        services.TryAddSingleton<IMessageRoutingResolver>(sp =>
            new MessageRoutingResolver(sp.GetRequiredService<ITopologyProvider>()));
    }

    /// <summary>
    /// Adds Redis Streams health checks.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="name">The health check name.</param>
    /// <param name="failureStatus">The failure status.</param>
    /// <param name="tags">Optional tags.</param>
    /// <returns>The messaging builder for chaining.</returns>
    public static IMessagingBuilder AddHealthChecks(
        this IMessagingBuilder builder,
        string name = "redis-streams",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
    {
        builder.Services.AddHealthChecks()
            .Add(new HealthCheckRegistration(
                name,
                sp => sp.GetRequiredService<RedisStreamsHealthCheck>(),
                failureStatus,
                tags));

        return builder;
    }

    /// <summary>
    /// Adds the Redis Streams consumer hosted service.
    /// This must be called AFTER AddTopology to ensure proper startup ordering.
    /// The consumer service waits for TopologyReadySignal which is set by TopologyInitializationHostedService.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <returns>The messaging builder for chaining.</returns>
    public static IMessagingBuilder AddRedisStreamsConsumerHostedService(this IMessagingBuilder builder)
    {
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<Microsoft.Extensions.Hosting.IHostedService, Hosting.RedisStreamsConsumerHostedService>());

        return builder;
    }
}

/// <summary>
/// Internal messaging builder implementation.
/// </summary>
internal sealed class MessagingBuilder : IMessagingBuilder
{
    public MessagingBuilder(IServiceCollection services)
    {
        Services = services;
    }

    public IServiceCollection Services { get; }
}
