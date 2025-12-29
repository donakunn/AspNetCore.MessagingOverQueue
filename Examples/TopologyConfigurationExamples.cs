using MessagingOverQueue.src.Abstractions.Consuming;
using MessagingOverQueue.src.DependencyInjection;
using MessagingOverQueue.src.Topology.Attributes;
using MessagingOverQueue.src.Topology.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

namespace MessagingOverQueue.Examples;

/// <summary>
/// Examples demonstrating the new handler-based topology configuration.
/// The library now scans for message handlers and automatically:
/// - Registers topology (exchanges, queues, bindings)
/// - Registers handlers in DI
/// - Sets up consumers for each handler's queue
/// </summary>
public static class TopologyConfigurationExamples
{
    /// <summary>
    /// Example 1: Handler-based auto-discovery (NEW - Recommended)
    /// Scans assemblies for IMessageHandler implementations and automatically configures everything.
    /// </summary>
    public static void ConfigureWithHandlerDiscovery(IServiceCollection services)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .UsePort(5672)
            .WithCredentials("guest", "guest"))
            // Add topology - scans for handlers, not message types
            .AddTopology(topology => topology
                // Set service name for queue naming
                .WithServiceName("order-service")
                // Enable dead letter queues by default
                .WithDeadLetterEnabled(true)
                // Scan assemblies for IMessageHandler<T> implementations
                .ScanAssemblyContaining<OrderCreatedEventHandler>()
                // Configure naming conventions
                .ConfigureNaming(naming =>
                {
                    naming.UseLowerCase = true;
                    naming.EventExchangePrefix = "events.";
                    naming.CommandExchangePrefix = "commands.";
                    naming.DeadLetterExchangePrefix = "dlx.";
                }));

        // This automatically:
        // 1. Finds OrderCreatedEventHandler : IMessageHandler<OrderCreatedEvent>
        // 2. Creates exchange: events.order-created (topic, durable)
        // 3. Creates queue: order-service.order-created (durable)
        // 4. Registers handler in DI
        // 5. Sets up consumer for the queue
    }

    /// <summary>
    /// Example 2: Handler with ConsumerQueueAttribute for custom queue configuration
    /// Use ConsumerQueueAttribute on handlers to customize the consumer queue.
    /// </summary>
    public static void ConfigureWithCustomConsumerQueue(IServiceCollection services)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .UsePort(5672)
            .WithCredentials("guest", "guest"))
            .AddTopology(topology => topology
                .WithServiceName("payment-service")
                .ScanAssemblyContaining<PaymentProcessedHandler>());

        // PaymentProcessedHandler uses [ConsumerQueue] attribute to customize:
        // - Custom queue name
        // - Quorum queue type
        // - Prefetch count
        // - Max concurrency
    }

    /// <summary>
    /// Example 3: Multiple handlers for the same event (different services)
    /// Each handler gets its own queue, all bound to the same exchange.
    /// </summary>
    public static void ConfigureMultipleHandlersForSameEvent(IServiceCollection services)
    {
        // In Notification Service
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .WithCredentials("guest", "guest"))
            .AddTopology(topology => topology
                .WithServiceName("notification-service")
                .ScanAssemblyContaining<NotifyOnOrderCreatedHandler>());
        // Queue: notification-service.order-created

        // In Analytics Service (different application)
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .WithCredentials("guest", "guest"))
            .AddTopology(topology => topology
                .WithServiceName("analytics-service")
                .ScanAssemblyContaining<TrackOrderCreatedHandler>());
        // Queue: analytics-service.order-created

        // Both queues bound to: events.order-created exchange
    }

    /// <summary>
    /// Example 4: Hybrid configuration - auto-discovery with manual overrides.
    /// </summary>
    public static void ConfigureWithHybridApproach(IServiceCollection services)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .WithCredentials("guest", "guest"))
            .AddTopology(topology => topology
                .WithServiceName("inventory-service")
                // Auto-discover handlers
                .ScanAssemblyContaining<InventoryEventHandler>()
                // But also add specific configurations that override defaults
                .AddTopology<OrderCreatedEvent>(msg => msg
                    .WithExchangeName("custom-orders")
                    .WithQueueName("inventory-order-events")
                    .WithRoutingKey("orders.created.inventory")));
    }

    /// <summary>
    /// Example 5: High-availability configuration with handler attributes.
    /// </summary>
    public static void ConfigureForHighAvailability(IServiceCollection services)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("rabbitmq.cluster.local")
            .WithCredentials("app-user", "secure-password")
            .WithConnectionName("ha-service"))
            .AddTopology(topology => topology
                .WithServiceName("critical-service")
                .WithDeadLetterEnabled(true)
                .ScanAssemblyContaining<CriticalPaymentHandler>()
                .ConfigureProvider(provider =>
                {
                    provider.DefaultDurable = true;
                    provider.EnableDeadLetterByDefault = true;
                }));

        // CriticalPaymentHandler uses [ConsumerQueue(QueueType = QueueType.Quorum)]
        // to ensure high availability
    }

    /// <summary>
    /// Example 6: Legacy message-type based discovery (for backward compatibility).
    /// </summary>
    public static void ConfigureWithLegacyDiscovery(IServiceCollection services)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .WithCredentials("guest", "guest"))
            .AddTopology(topology => topology
                .WithServiceName("legacy-service")
                // Use old behavior - scan for message types
                .UseMessageTypeDiscovery()
                .ScanAssemblyContaining<OrderCreatedEvent>())
            // With legacy mode, you need to manually register handlers and consumers
            .AddHandler<OrderCreatedEventHandler, OrderCreatedEvent>()
            .AddConsumer("legacy-service.order-created", options =>
            {
                options.PrefetchCount = 10;
                options.MaxConcurrency = 5;
            });
    }

    /// <summary>
    /// Example 7: Manual-only configuration (no auto-discovery).
    /// </summary>
    public static void ConfigureManualOnly(IServiceCollection services)
    {
        services.AddRabbitMqMessaging(options => options
            .UseHost("localhost")
            .WithCredentials("guest", "guest"))
            .AddTopology(topology => topology
                .DisableAutoDiscovery()
                .AddTopology<OrderCreatedEvent>(msg => msg
                    .WithExchangeName("orders")
                    .WithQueueName("order-events")
                    .WithRoutingKey("orders.created"))
                .AddTopology<CreateOrderCommand>(msg => msg
                    .WithExchangeName("commands")
                    .WithQueueName("create-order")
                    .WithRoutingKey("orders.create")))
            .AddHandler<OrderCreatedEventHandler, OrderCreatedEvent>()
            .AddConsumer("order-events");
    }
}

// ============================================================================
// Example Handler Classes
// ============================================================================

/// <summary>
/// Basic handler - uses convention-based queue naming.
/// Queue name will be: {service-name}.{message-name}
/// </summary>
internal class OrderCreatedEventHandler : IMessageHandler<OrderCreatedEvent>
{
    public Task HandleAsync(OrderCreatedEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        // Handle the event
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler with custom queue configuration using ConsumerQueueAttribute.
/// </summary>
[ConsumerQueue(
    Name = "critical-payments",
    QueueType = QueueType.Quorum,
    PrefetchCount = 20,
    MaxConcurrency = 5)]
internal class PaymentProcessedHandler : IMessageHandler<PaymentProcessedEvent>
{
    public Task HandleAsync(PaymentProcessedEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler with high-availability queue type.
/// </summary>
[ConsumerQueue(QueueType = QueueType.Quorum, PrefetchCount = 50, MaxConcurrency = 10)]
internal class CriticalPaymentHandler : IMessageHandler<PaymentProcessedEvent>
{
    public Task HandleAsync(PaymentProcessedEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler for notifications - different service, same event.
/// </summary>
internal class NotifyOnOrderCreatedHandler : IMessageHandler<OrderCreatedEvent>
{
    public Task HandleAsync(OrderCreatedEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        // Send notification
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler for analytics - different service, same event.
/// </summary>
internal class TrackOrderCreatedHandler : IMessageHandler<OrderCreatedEvent>
{
    public Task HandleAsync(OrderCreatedEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        // Track analytics
        return Task.CompletedTask;
    }
}

/// <summary>
/// Generic inventory handler placeholder.
/// </summary>
internal class InventoryEventHandler : IMessageHandler<OrderCreatedEvent>
{
    public Task HandleAsync(OrderCreatedEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
