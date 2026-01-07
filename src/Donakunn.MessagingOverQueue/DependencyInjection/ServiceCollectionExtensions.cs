using MessagingOverQueue.src.Abstractions.Consuming;
using MessagingOverQueue.src.Abstractions.Messages;
using MessagingOverQueue.src.Abstractions.Publishing;
using MessagingOverQueue.src.Abstractions.Serialization;
using MessagingOverQueue.src.Configuration.Builders;
using MessagingOverQueue.src.Configuration.Options;
using MessagingOverQueue.src.Configuration.Sources;
using MessagingOverQueue.src.Connection;
using MessagingOverQueue.src.Consuming.Handlers;
using MessagingOverQueue.src.Consuming.Middleware;
using MessagingOverQueue.src.HealthChecks;
using MessagingOverQueue.src.Hosting;
using MessagingOverQueue.src.Persistence;
using MessagingOverQueue.src.Persistence.Repositories;
using MessagingOverQueue.src.Publishing;
using MessagingOverQueue.src.Publishing.Middleware;
using MessagingOverQueue.src.Resilience;
using MessagingOverQueue.src.Resilience.CircuitBreaker;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace MessagingOverQueue.src.DependencyInjection;

/// <summary>
/// Extension methods for registering RabbitMQ messaging services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds RabbitMQ messaging services using fluent configuration.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configuration action for RabbitMQ options.</param>
    /// <returns>The messaging builder for further configuration.</returns>
    public static IMessagingBuilder AddRabbitMqMessaging(
        this IServiceCollection services,
        Action<RabbitMqOptionsBuilder> configure)
    {
        var composer = new RabbitMqConfigurationComposer()
            .AddSource(new FluentConfigurationSource(configure));

        return services.AddRabbitMqMessagingCore(composer);
    }

    /// <summary>
    /// Adds RabbitMQ messaging services using IConfiguration (appsettings.json).
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The configuration instance.</param>
    /// <param name="sectionName">The configuration section name (default: "RabbitMq").</param>
    /// <returns>The messaging builder for further configuration.</returns>
    public static IMessagingBuilder AddRabbitMqMessaging(
        this IServiceCollection services,
        IConfiguration configuration,
        string? sectionName = null)
    {
        var composer = new RabbitMqConfigurationComposer()
            .AddSource(new AppSettingsConfigurationSource(configuration, sectionName));

        return services.AddRabbitMqMessagingCore(composer);
    }

    /// <summary>
    /// Adds RabbitMQ messaging services using IConfiguration and fluent configuration.
    /// Fluent configuration takes precedence over appsettings.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The configuration instance.</param>
    /// <param name="configure">Additional fluent configuration.</param>
    /// <param name="sectionName">The configuration section name (default: "RabbitMq").</param>
    /// <returns>The messaging builder for further configuration.</returns>
    public static IMessagingBuilder AddRabbitMqMessaging(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<RabbitMqOptionsBuilder> configure,
        string? sectionName = null)
    {
        var composer = new RabbitMqConfigurationComposer()
            .AddSource(new AppSettingsConfigurationSource(configuration, sectionName))
            .AddSource(new FluentConfigurationSource(configure));

        return services.AddRabbitMqMessagingCore(composer);
    }

    /// <summary>
    /// Adds RabbitMQ messaging services using .NET Aspire service discovery.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The configuration instance.</param>
    /// <param name="connectionName">The Aspire connection name (default: "rabbitmq").</param>
    /// <returns>The messaging builder for further configuration.</returns>
    public static IMessagingBuilder AddRabbitMqMessagingFromAspire(
        this IServiceCollection services,
        IConfiguration configuration,
        string connectionName = "rabbitmq")
    {
        var composer = new RabbitMqConfigurationComposer()
            .AddSource(new AspireConfigurationSource(configuration, connectionName));

        return services.AddRabbitMqMessagingCore(composer);
    }

    /// <summary>
    /// Adds RabbitMQ messaging services using .NET Aspire with fluent override.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The configuration instance.</param>
    /// <param name="configure">Additional fluent configuration.</param>
    /// <param name="connectionName">The Aspire connection name (default: "rabbitmq").</param>
    /// <returns>The messaging builder for further configuration.</returns>
    public static IMessagingBuilder AddRabbitMqMessagingFromAspire(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<RabbitMqOptionsBuilder> configure,
        string connectionName = "rabbitmq")
    {
        var composer = new RabbitMqConfigurationComposer()
            .AddSource(new AspireConfigurationSource(configuration, connectionName))
            .AddSource(new FluentConfigurationSource(configure));

        return services.AddRabbitMqMessagingCore(composer);
    }

    /// <summary>
    /// Adds RabbitMQ messaging services using custom configuration sources.
    /// This allows complete control over configuration composition.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureSources">Action to configure the composer with custom sources.</param>
    /// <returns>The messaging builder for further configuration.</returns>
    public static IMessagingBuilder AddRabbitMqMessaging(
        this IServiceCollection services,
        Action<RabbitMqConfigurationComposer> configureSources)
    {
        var composer = new RabbitMqConfigurationComposer();
        configureSources(composer);

        return services.AddRabbitMqMessagingCore(composer);
    }

    /// <summary>
    /// Adds RabbitMQ messaging with default configuration.
    /// </summary>
    public static IMessagingBuilder AddRabbitMqMessaging(this IServiceCollection services)
    {
        var composer = new RabbitMqConfigurationComposer()
            .AddSource(new FluentConfigurationSource(_ => { }));

        return services.AddRabbitMqMessagingCore(composer);
    }

    /// <summary>
    /// Core method that registers all RabbitMQ services.
    /// Does NOT register topology or handler scanning - use AddTopology() for that.
    /// </summary>
    private static IMessagingBuilder AddRabbitMqMessagingCore(
        this IServiceCollection services,
        RabbitMqConfigurationComposer composer)
    {
        // Configure options using the composer
        services.Configure(composer.CreateConfigureAction());

        // Core connection services
        services.TryAddSingleton<IRabbitMqConnectionPool, RabbitMqConnectionPool>();

        // Serialization services
        services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.TryAddSingleton<IMessageTypeResolver, MessageTypeResolver>();

        // Handler invoker infrastructure (populated by AddTopology)
        services.TryAddSingleton<IHandlerInvokerRegistry, HandlerInvokerRegistry>();
        services.TryAddSingleton<IHandlerInvokerFactory, HandlerInvokerFactory>();

        // Publishers
        services.TryAddSingleton<RabbitMqPublisher>();
        services.TryAddSingleton<IMessagePublisher>(sp => sp.GetRequiredService<RabbitMqPublisher>());
        services.TryAddSingleton<IEventPublisher>(sp => sp.GetRequiredService<RabbitMqPublisher>());
        services.TryAddSingleton<ICommandSender>(sp => sp.GetRequiredService<RabbitMqPublisher>());

        // Publish middleware
        services.AddSingleton<IPublishMiddleware, LoggingMiddleware>();
        services.AddSingleton<IPublishMiddleware, SerializationMiddleware>();

        // Consume middleware
        services.AddSingleton<IConsumeMiddleware, ConsumeLoggingMiddleware>();
        services.AddSingleton<IConsumeMiddleware, DeserializationMiddleware>();

        // Resilience
        services.Configure<RetryOptions>(_ => { });
        services.TryAddSingleton<IRetryPolicy, PollyRetryPolicy>();

        // Connection management hosted service
        services.AddHostedService<RabbitMqHostedService>();

        return new MessagingBuilder(services);
    }

    /// <summary>
    /// Adds the outbox pattern for reliable messaging.
    /// </summary>
    /// <typeparam name="TContext">The DbContext type that implements IOutboxDbContext.</typeparam>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="configure">Optional configuration for outbox options.</param>
    /// <returns>The messaging builder for chaining.</returns>
    public static IMessagingBuilder AddOutboxPattern<TContext>(
        this IMessagingBuilder builder,
        Action<OutboxOptions>? configure = null)
        where TContext : DbContext, IOutboxDbContext
    {
        var services = builder.Services;

        services.Configure<OutboxOptions>(options =>
        {
            configure?.Invoke(options);
        });

        // Register outbox repository
        services.AddScoped<IOutboxRepository, OutboxRepository<TContext>>();

        // Register outbox publisher as the default publisher for transactional scenarios
        services.AddScoped<OutboxPublisher>();

        // Register outbox processor
        services.AddHostedService<OutboxProcessor>();

        return builder;
    }

    /// <summary>
    /// Adds a message handler to the service collection manually.
    /// For automatic handler discovery, use AddTopology() instead.
    /// </summary>
    /// <typeparam name="THandler">The handler type.</typeparam>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="builder">The messaging builder.</param>
    /// <returns>The messaging builder for chaining.</returns>
    public static IMessagingBuilder AddHandler<THandler, TMessage>(this IMessagingBuilder builder)
        where THandler : class, IMessageHandler<TMessage>
        where TMessage : IMessage
    {
        builder.Services.AddScoped<IMessageHandler<TMessage>, THandler>();
        builder.Services.AddSingleton<IHandlerInvokerRegistration>(new HandlerInvokerRegistration<TMessage>());

        return builder;
    }

    /// <summary>
    /// Adds a consumer for a specific queue manually.
    /// For automatic consumer setup based on handlers, use AddTopology() instead.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="queueName">The queue to consume from.</param>
    /// <param name="configure">Optional consumer configuration.</param>
    /// <returns>The messaging builder for chaining.</returns>
    public static IMessagingBuilder AddConsumer(
        this IMessagingBuilder builder,
        string queueName,
        Action<ConsumerOptions>? configure = null)
    {
        var options = new ConsumerOptions { QueueName = queueName };
        configure?.Invoke(options);

        builder.Services.AddSingleton(new ConsumerRegistration { Options = options });

        // Ensure consumer hosted service is registered
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<Microsoft.Extensions.Hosting.IHostedService, ConsumerHostedService>());

        return builder;
    }

    /// <summary>
    /// Adds RabbitMQ health checks.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="name">The health check name.</param>
    /// <param name="failureStatus">The failure status.</param>
    /// <param name="tags">Optional tags.</param>
    /// <returns>The messaging builder for chaining.</returns>
    public static IMessagingBuilder AddHealthChecks(
        this IMessagingBuilder builder,
        string name = "rabbitmq",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
    {
        builder.Services.AddHealthChecks()
            .Add(new HealthCheckRegistration(
                name,
                sp => sp.GetRequiredService<RabbitMqHealthCheck>(),
                failureStatus,
                tags));

        builder.Services.TryAddSingleton<RabbitMqHealthCheck>();

        return builder;
    }

    /// <summary>
    /// Configures retry options.
    /// </summary>
    public static IMessagingBuilder ConfigureRetry(
        this IMessagingBuilder builder,
        Action<RetryOptions> configure)
    {
        builder.Services.Configure(configure);
        return builder;
    }

    /// <summary>
    /// Adds a circuit breaker.
    /// </summary>
    public static IMessagingBuilder AddCircuitBreaker(
        this IMessagingBuilder builder,
        Action<CircuitBreakerOptions>? configure = null)
    {
        var options = new CircuitBreakerOptions();
        configure?.Invoke(options);

        builder.Services.AddSingleton<ICircuitBreaker>(new PollyCircuitBreaker(options));
        return builder;
    }
}

/// <summary>
/// Builder interface for configuring messaging services.
/// </summary>
public interface IMessagingBuilder
{
    /// <summary>
    /// The service collection.
    /// </summary>
    IServiceCollection Services { get; }
}

/// <summary>
/// Default implementation of IMessagingBuilder.
/// </summary>
internal sealed class MessagingBuilder : IMessagingBuilder
{
    public IServiceCollection Services { get; }

    public MessagingBuilder(IServiceCollection services)
    {
        Services = services;
    }
}

/// <summary>
/// Interface for handler invoker registrations.
/// </summary>
public interface IHandlerInvokerRegistration
{
    /// <summary>
    /// The message type this registration is for.
    /// </summary>
    Type MessageType { get; }

    /// <summary>
    /// Registers the handler invoker with the registry.
    /// </summary>
    void Register(IHandlerInvokerRegistry registry, IHandlerInvokerFactory factory);
}

/// <summary>
/// Registration for a handler invoker.
/// </summary>
public sealed class HandlerInvokerRegistration<TMessage> : IHandlerInvokerRegistration
    where TMessage : IMessage
{
    public Type MessageType => typeof(TMessage);

    public void Register(IHandlerInvokerRegistry registry, IHandlerInvokerFactory factory)
    {
        if (!registry.IsRegistered(typeof(TMessage)))
        {
            var invoker = factory.Create(typeof(TMessage));
            registry.Register(invoker);
        }
    }
}

