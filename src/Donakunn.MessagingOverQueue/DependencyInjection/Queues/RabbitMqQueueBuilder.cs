using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.Abstractions.Serialization;
using Donakunn.MessagingOverQueue.Configuration.Builders;
using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Configuration.Sources;
using Donakunn.MessagingOverQueue.Connection;
using Donakunn.MessagingOverQueue.Consuming.Handlers;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.HealthChecks;
using Donakunn.MessagingOverQueue.Hosting;
using Donakunn.MessagingOverQueue.Providers;
using Donakunn.MessagingOverQueue.Publishing;
using Donakunn.MessagingOverQueue.Publishing.Middleware;
using Donakunn.MessagingOverQueue.Resilience;
using Donakunn.MessagingOverQueue.Topology.Builders;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Donakunn.MessagingOverQueue.DependencyInjection.Queues;

/// <summary>
/// Implementation of the RabbitMQ queue builder.
/// </summary>
internal sealed class RabbitMqQueueBuilder : IRabbitMqQueueBuilder
{
    private readonly RabbitMqConfigurationComposer _composer = new();
    private bool _coreServicesRegistered;

    public IServiceCollection Services { get; }
    public IMessagingBuilder MessagingBuilder { get; }

    public RabbitMqQueueBuilder(IServiceCollection services, IMessagingBuilder messagingBuilder)
    {
        Services = services;
        MessagingBuilder = messagingBuilder;
    }

    public IRabbitMqQueueBuilder WithConnection(Action<RabbitMqOptionsBuilder> configure)
    {
        _composer.AddSource(new FluentConfigurationSource(configure));
        EnsureCoreServicesRegistered();
        return this;
    }

    public IRabbitMqQueueBuilder WithTopology(Action<TopologyBuilder> configure)
    {
        EnsureCoreServicesRegistered();
        MessagingBuilder.AddTopology(configure);
        return this;
    }

    public IRabbitMqQueueBuilder WithHealthChecks(
        string name = "rabbitmq",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
    {
        EnsureCoreServicesRegistered();

        Services.AddHealthChecks()
            .Add(new HealthCheckRegistration(
                name,
                sp => sp.GetRequiredService<RabbitMqHealthCheck>(),
                failureStatus,
                tags));

        Services.TryAddSingleton<RabbitMqHealthCheck>();

        return this;
    }

    /// <summary>
    /// Ensures core RabbitMQ services are registered.
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

        // Configure options using the composer
        Services.Configure(_composer.CreateConfigureAction());

        // Core connection services
        Services.TryAddSingleton<IRabbitMqConnectionPool, RabbitMqConnectionPool>();

        // Register messaging provider abstraction for RabbitMQ
        Services.TryAddSingleton<IMessagingProvider, Providers.RabbitMq.RabbitMqMessagingProvider>();

        // Serialization services
        Services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();
        Services.TryAddSingleton<IMessageTypeResolver, MessageTypeResolver>();

        // Handler invoker infrastructure (populated by AddTopology)
        Services.TryAddSingleton<IHandlerInvokerRegistry, HandlerInvokerRegistry>();
        Services.TryAddSingleton<IHandlerInvokerFactory, HandlerInvokerFactory>();

        // Publishers
        Services.TryAddSingleton<RabbitMqPublisher>();
        Services.TryAddSingleton<IMessagePublisher>(sp => sp.GetRequiredService<RabbitMqPublisher>());
        Services.TryAddSingleton<IEventPublisher>(sp => sp.GetRequiredService<RabbitMqPublisher>());
        Services.TryAddSingleton<ICommandSender>(sp => sp.GetRequiredService<RabbitMqPublisher>());

        // Publish middleware
        Services.AddSingleton<IPublishMiddleware, LoggingMiddleware>();
        Services.AddSingleton<IPublishMiddleware, SerializationMiddleware>();

        // Consume middleware (core - logging and deserialization)
        Services.AddSingleton<IConsumeMiddleware, ConsumeLoggingMiddleware>();
        Services.AddSingleton<IConsumeMiddleware, DeserializationMiddleware>();

        // Resilience defaults
        Services.Configure<RetryOptions>(_ => { });
        Services.TryAddSingleton<IRetryPolicy, PollyRetryPolicy>();

        // Connection management hosted service
        Services.AddHostedService<RabbitMqHostedService>();

        // Consumer hosted service for RabbitMQ
        Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<Microsoft.Extensions.Hosting.IHostedService, ConsumerHostedService>());
    }
}
