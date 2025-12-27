using MessagingOverQueue.Abstractions.Messages;
using MessagingOverQueue.Topology.Attributes;

namespace MessagingOverQueue.Examples.Messages;

// ==========================================
// Basic Event with Default Conventions
// ==========================================
// Exchange: events.order-created (topic)
// Queue: {service-name}.order-created
// Routing Key: orders.order.created

/// <summary>
/// Event using default conventions.
/// Exchange and queue names are derived from the class name.
/// </summary>
public class OrderCreatedEvent : Event
{
    public Guid OrderId { get; init; }
    public string CustomerId { get; init; } = string.Empty;
    public decimal TotalAmount { get; init; }
}

// ==========================================
// Command with Default Conventions
// ==========================================
// Exchange: commands.create-order (direct)
// Queue: create-order
// Routing Key: create.order

/// <summary>
/// Command using default conventions.
/// Commands use direct exchange for point-to-point messaging.
/// </summary>
public class CreateOrderCommand : Command
{
    public string CustomerId { get; init; } = string.Empty;
    public List<OrderItem> Items { get; init; } = new();
}

public class OrderItem
{
    public string ProductId { get; init; } = string.Empty;
    public int Quantity { get; init; }
    public decimal UnitPrice { get; init; }
}

// ==========================================
// Event with Custom Exchange Configuration
// ==========================================

/// <summary>
/// Event with custom exchange name and type.
/// </summary>
[Exchange("custom-orders-exchange", Type = ExchangeType.Topic)]
[Queue("order-shipped-queue")]
[RoutingKey("orders.shipped")]
public class OrderShippedEvent : Event
{
    public Guid OrderId { get; init; }
    public string TrackingNumber { get; init; } = string.Empty;
    public DateTime ShippedAt { get; init; }
}

// ==========================================
// Event with Dead Letter Configuration
// ==========================================

/// <summary>
/// Event with custom dead letter configuration.
/// </summary>
[Exchange("payments-exchange")]
[Queue("payment-processed-queue", MessageTtlMs = 86400000)] // 24 hours TTL
[DeadLetter("payments-dlx", QueueName = "payments-failed")]
[RetryPolicy(MaxRetries = 5, InitialDelayMs = 2000)]
public class PaymentProcessedEvent : Event
{
    public Guid PaymentId { get; init; }
    public Guid OrderId { get; init; }
    public decimal Amount { get; init; }
    public string Status { get; init; } = string.Empty;
}

// ==========================================
// Command with Quorum Queue
// ==========================================

/// <summary>
/// Command using quorum queue for high availability.
/// </summary>
[Queue("process-refund-queue", QueueType = QueueType.Quorum)]
[DeadLetter(Enabled = true)]
public class ProcessRefundCommand : Command
{
    public Guid OrderId { get; init; }
    public decimal RefundAmount { get; init; }
    public string Reason { get; init; } = string.Empty;
}

// ==========================================
// Event with Fanout Exchange
// ==========================================

/// <summary>
/// Event using fanout exchange for broadcast to all subscribers.
/// </summary>
[Exchange("system-notifications", Type = ExchangeType.Fanout)]
[Queue("system-alerts-queue")]
public class SystemAlertEvent : Event
{
    public string AlertType { get; init; } = string.Empty;
    public string Message { get; init; } = string.Empty;
    public string Severity { get; init; } = string.Empty;
}

// ==========================================
// Event with Multiple Bindings
// ==========================================

/// <summary>
/// Event with multiple routing key bindings.
/// </summary>
[Exchange("audit-exchange", Type = ExchangeType.Topic)]
[Queue("audit-events-queue")]
[Binding("audit.user.*")]
[Binding("audit.order.*")]
[Binding("audit.payment.*")]
public class AuditLogEvent : Event
{
    public string EntityType { get; init; } = string.Empty;
    public string EntityId { get; init; } = string.Empty;
    public string Action { get; init; } = string.Empty;
    public string UserId { get; init; } = string.Empty;
    public Dictionary<string, object> Changes { get; init; } = new();
}

// ==========================================
// Event Disabled from Auto-Discovery
// ==========================================

/// <summary>
/// Event that is excluded from auto-discovery.
/// Must be registered manually if needed.
/// </summary>
[Message(AutoDiscover = false)]
public class InternalDiagnosticEvent : Event
{
    public string Component { get; init; } = string.Empty;
    public string Metric { get; init; } = string.Empty;
    public double Value { get; init; }
}

// ==========================================
// Event with Stream Queue (High Throughput)
// ==========================================

/// <summary>
/// Event using stream queue for high-throughput scenarios.
/// </summary>
[Queue("telemetry-stream", QueueType = QueueType.Stream)]
[Exchange("telemetry-exchange", Type = ExchangeType.Topic)]
[RoutingKey("telemetry.#")]
[DeadLetter(Enabled = false)] // Stream queues don't support DLX
public class TelemetryDataEvent : Event
{
    public string DeviceId { get; init; } = string.Empty;
    public string MetricName { get; init; } = string.Empty;
    public double Value { get; init; }
    public DateTime RecordedAt { get; init; }
}

// ==========================================
// Event with Lazy Queue
// ==========================================

/// <summary>
/// Event using lazy queue for memory-efficient large queues.
/// </summary>
[Queue("bulk-import-queue", QueueType = QueueType.Lazy, MaxLength = 1000000)]
[Exchange("import-exchange")]
public class BulkImportRecordEvent : Event
{
    public int RecordIndex { get; init; }
    public string ImportJobId { get; init; } = string.Empty;
    public Dictionary<string, string> Data { get; init; } = new();
}

// ==========================================
// Event with Headers Exchange
// ==========================================

/// <summary>
/// Event using headers exchange for attribute-based routing.
/// </summary>
[Exchange("notifications-headers", Type = ExchangeType.Headers)]
[Queue("email-notifications")]
public class NotificationEvent : Event
{
    public string NotificationType { get; init; } = string.Empty;
    public string Channel { get; init; } = string.Empty;
    public string Recipient { get; init; } = string.Empty;
    public string Subject { get; init; } = string.Empty;
    public string Body { get; init; } = string.Empty;
}

// ==========================================
// Versioned Event
// ==========================================

/// <summary>
/// Event with version information for schema evolution.
/// </summary>
[Message(Version = "2.0", Group = "inventory")]
[Exchange("inventory-exchange")]
[Queue("inventory-updated-v2-queue")]
public class InventoryUpdatedEventV2 : Event
{
    public string ProductId { get; init; } = string.Empty;
    public int QuantityChange { get; init; }
    public int NewQuantity { get; init; }
    public string WarehouseId { get; init; } = string.Empty;
    public string Reason { get; init; } = string.Empty;
}
