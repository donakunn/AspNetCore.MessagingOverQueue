using MessagingOverQueue.DependencyInjection;
using MessagingOverQueue.Examples.Messages;
using MessagingOverQueue.Topology.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

namespace MessagingOverQueue.Examples;

/// <summary>
/// Examples demonstrating topology configuration patterns.
/// </summary>
public static class TopologyConfigurationExamples
{
    /// <summary>
    /// Example 1: Auto-discovery with assembly scanning.
    /// Scans assemblies for message types and automatically creates topology.
    /// </summary>
    public static void ConfigureWithAutoDiscovery(IServiceCollection services)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .UsePort(5672)
            .WithCredentials("guest", "guest"))
            // Add topology with auto-discovery
            .AddTopology(topology => topology
                // Set service name for queue naming
                .WithServiceName("order-service")
                // Enable dead letter queues by default
                .WithDeadLetterEnabled(true)
                // Scan assemblies for message types
                .ScanAssemblyContaining<OrderCreatedEvent>()
                // Configure naming conventions
                .ConfigureNaming(naming =>
                {
                    naming.UseLowerCase = true;
                    naming.EventExchangePrefix = "events.";
                    naming.CommandExchangePrefix = "commands.";
                    naming.DeadLetterExchangePrefix = "dlx.";
                }));
    }

    /// <summary>
    /// Example 2: Manual topology configuration with fluent API.
    /// Provides explicit control over exchange, queue, and binding configuration.
    /// </summary>
    public static void ConfigureWithFluentApi(IServiceCollection services)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .UsePort(5672)
            .WithCredentials("guest", "guest"))
            .AddTopology(topology => topology
                .WithServiceName("payment-service")
                // Configure specific message topology
                .AddTopology<PaymentProcessedEvent>(msg => msg
                    .WithExchange(ex => ex
                        .WithName("payments")
                        .AsTopic()
                        .Durable())
                    .WithQueue(q => q
                        .WithName("payment-events")
                        .Durable()
                        .WithMessageTtl(TimeSpan.FromDays(7))
                        .AsQuorumQueue())
                    .WithRoutingKey("payments.processed")
                    .WithDeadLetter(dl => dl
                        .WithExchange("payments-dlx")
                        .WithQueue("payments-failed")))
                // Add another message topology
                .AddTopology<ProcessRefundCommand>(msg => msg
                    .WithExchangeName("refunds")
                    .WithQueueName("refund-processing")
                    .WithRoutingKey("refunds.process")
                    .WithDeadLetter()));
    }

    /// <summary>
    /// Example 3: Hybrid configuration - auto-discovery with overrides.
    /// Uses auto-discovery as baseline and allows specific overrides.
    /// </summary>
    public static void ConfigureWithHybridApproach(IServiceCollection services)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .UsePort(5672)
            .WithCredentials("guest", "guest"))
            .AddTopology(topology => topology
                .WithServiceName("inventory-service")
                // Enable auto-discovery
                .ScanAssemblyContaining<OrderCreatedEvent>()
                // But also add specific configurations that override defaults
                .AddTopology<OrderCreatedEvent>(msg => msg
                    .WithExchangeName("custom-orders")
                    .WithQueueName("inventory-order-events")
                    .WithRoutingKey("orders.created.inventory")));
    }

    /// <summary>
    /// Example 4: Configuration for high-availability scenarios.
    /// Uses quorum queues and proper dead letter configuration.
    /// </summary>
    public static void ConfigureForHighAvailability(IServiceCollection services)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("rabbitmq.cluster.local")
            .UsePort(5672)
            .WithCredentials("app-user", "secure-password")
            .WithConnectionName("ha-service"))
            .AddTopology(topology => topology
                .WithServiceName("critical-service")
                .WithDeadLetterEnabled(true)
                .ScanAssemblyContaining<OrderCreatedEvent>()
                // Configure provider for HA defaults
                .ConfigureProvider(provider =>
                {
                    provider.DefaultDurable = true;
                    provider.EnableDeadLetterByDefault = true;
                })
                // Override specific messages for HA
                .AddTopology<PaymentProcessedEvent>(msg => msg
                    .WithQueue(q => q
                        .WithName("critical-payments")
                        .Durable()
                        .AsQuorumQueue()
                        .WithMaxLength(1000000))
                    .WithDeadLetter(dl => dl
                        .WithExchange("critical-dlx")
                        .WithQueue("critical-failed"))));
    }

    /// <summary>
    /// Example 5: Configuration for high-throughput scenarios.
    /// Uses stream queues and optimized settings.
    /// </summary>
    public static void ConfigureForHighThroughput(IServiceCollection services)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .UsePort(5672)
            .WithCredentials("guest", "guest")
            .WithChannelPoolSize(20)) // Larger pool for concurrency
            .AddTopology(topology => topology
                .WithServiceName("telemetry-collector")
                // Disable dead letter for stream queues (not supported)
                .WithDeadLetterEnabled(false)
                .AddTopology<TelemetryDataEvent>(msg => msg
                    .WithExchange(ex => ex
                        .WithName("telemetry")
                        .AsTopic()
                        .Durable())
                    .WithQueue(q => q
                        .WithName("telemetry-stream")
                        .Durable()
                        .AsStreamQueue())
                    .WithRoutingKey("telemetry.#")
                    .WithoutDeadLetter()));
    }

    /// <summary>
    /// Example 6: Multi-service topology with event subscriptions.
    /// Different services subscribe to the same events with their own queues.
    /// </summary>
    public static void ConfigureForMultiServiceSubscription(IServiceCollection services, string serviceName)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .UsePort(5672)
            .WithCredentials("guest", "guest"))
            .AddTopology(topology => topology
                // Each service gets its own queue for the same events
                .WithServiceName(serviceName)
                .ConfigureNaming(naming =>
                {
                    // Queue format: {service-name}.{event-name}
                    naming.ServiceName = serviceName;
                })
                .ScanAssemblyContaining<OrderCreatedEvent>());
        
        // Result for "order-service": queue = "order-service.order-created"
        // Result for "notification-service": queue = "notification-service.order-created"
        // Both queues bound to same exchange with same routing key
    }

    /// <summary>
    /// Example 7: Disable auto-discovery and use only manual configuration.
    /// </summary>
    public static void ConfigureManualOnly(IServiceCollection services)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .UsePort(5672)
            .WithCredentials("guest", "guest"))
            .AddTopology(topology => topology
                // Disable auto-discovery
                .DisableAutoDiscovery()
                // Manually configure only the topologies you need
                .AddTopology<OrderCreatedEvent>(msg => msg
                    .WithExchangeName("orders")
                    .WithQueueName("order-events")
                    .WithRoutingKey("orders.created"))
                .AddTopology<CreateOrderCommand>(msg => msg
                    .WithExchangeName("commands")
                    .WithQueueName("create-order")
                    .WithRoutingKey("orders.create")));
    }

    /// <summary>
    /// Example 8: Using topology with handler registration.
    /// Combines topology configuration with message handlers.
    /// </summary>
    public static void ConfigureWithHandlers(IServiceCollection services)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .UsePort(5672)
            .WithCredentials("guest", "guest"))
            // Register topology
            .AddTopology(topology => topology
                .WithServiceName("handler-service")
                .ScanAssemblyContaining<OrderCreatedEvent>())
            // Register handlers - topology auto-configures consumers
            .AddHandler<OrderCreatedEventHandler, OrderCreatedEvent>()
            // Add consumer for the queue
            .AddConsumer("handler-service.order-created", options =>
            {
                options.PrefetchCount = 10;
                options.MaxConcurrency = 5;
            })
            // Add health checks
            .AddHealthChecks();
    }
}

// Example handler (placeholder)
internal class OrderCreatedEventHandler : Abstractions.Consuming.IMessageHandler<OrderCreatedEvent>
{
    public Task HandleAsync(OrderCreatedEvent message, Abstractions.Consuming.IMessageContext context, CancellationToken cancellationToken)
    {
        // Handle the event
        return Task.CompletedTask;
    }
}
