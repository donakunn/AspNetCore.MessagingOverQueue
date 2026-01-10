# MessagingOverQueue - RabbitMQ Messaging Library for .NET

[![GitHub Repository](https://img.shields.io/badge/GitHub-donakunn%2FDonakunn.MessagingOverQueue-blue?logo=github)](https://github.com/donakunn/Donakunn.MessagingOverQueue)
[![.NET 10](https://img.shields.io/badge/.NET-10-purple)](https://dotnet.microsoft.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green)](LICENSE)

A robust, high-performance asynchronous messaging library for .NET 10 built on RabbitMQ with automatic handler-based topology discovery and SOLID design principles.

## Introduction

**MessagingOverQueue** is a production-ready RabbitMQ messaging library designed to eliminate boilerplate code and streamline message-driven architecture in .NET applications. Built with modern .NET best practices, it provides a developer-friendly abstraction over RabbitMQ while maintaining full control and flexibility.

### Why MessagingOverQueue?

Traditional RabbitMQ integration requires significant boilerplate: manual exchange and queue declarations, binding configuration, consumer setup, handler registration, and serialization plumbing. MessagingOverQueue eliminates this complexity through **intelligent handler-based auto-discovery** - simply implement `IMessageHandler<T>`, and the library automatically:

- **Discovers your handlers** at startup via assembly scanning
- **Creates RabbitMQ topology** (exchanges, queues, bindings) based on conventions or attributes
- **Registers handlers in DI** with scoped lifetime management
- **Sets up consumers** with optimized concurrency and prefetch settings
- **Dispatches messages** using reflection-free, strongly-typed handler invocation
- **Manages connections** with pooling and automatic recovery

### Architecture Highlights

**Reflection-Free Handler Dispatch**: Unlike traditional approaches that use reflection for every message, MessagingOverQueue employs a **handler invoker registry** pattern. Generic `HandlerInvoker<TMessage>` instances are created once at startup and cached in a `ConcurrentDictionary`, providing O(1) lookup and zero reflection overhead during message processing.

**Middleware Pipeline**: Extensible middleware architecture for both publishing and consuming, enabling cross-cutting concerns like logging, serialization, validation, and enrichment.

**Connection Pooling**: Dedicated channel pool with automatic recovery, ensuring high throughput and fault tolerance.

**Topology Management**: Supports convention-based auto-discovery, attribute-based configuration, fluent API, or hybrid approaches for maximum flexibility.

**Provider-Based Persistence**: Pluggable database provider architecture for the Outbox pattern. Choose your database (SQL Server, PostgreSQL, etc.) without being locked into Entity Framework Core.

### Target Scenarios

- **Microservices Communication**: Event-driven architectures, service-to-service messaging, CQRS implementations
- **Background Processing**: Asynchronous job queues, long-running tasks, scheduled workflows
- **Event Sourcing**: Publishing domain events with reliable delivery guarantees
- **Integration Patterns**: Message routing, pub/sub, request/reply, scatter-gather
- **High-Throughput Systems**: Optimized for concurrent message processing with configurable prefetch and parallelism

---

## Features

- 🚀 **Handler-Based Auto-Discovery**: Automatically configure topology by scanning for message handlers - exchanges, queues, bindings, and consumers are all set up automatically
- ⚡ **Reflection-Free Dispatch**: Handler invoker registry eliminates reflection overhead during message processing
- 🎯 **Clean Abstractions**: Simple interfaces for publishing and consuming messages (`ICommand`, `IEvent`, `IQuery`)
- ⚙️ **Flexible Configuration**: Multiple configuration sources - Fluent API, appsettings.json, .NET Aspire, or custom sources
- 🔄 **Provider-Based Outbox Pattern**: Reliable message delivery with pluggable database providers (SQL Server, with extensibility for others)
- 🛡️ **Resilience**: Built-in retry policies, circuit breakers, and dead letter handling
- 🔌 **Middleware Pipeline**: Extensible pipeline for both publishing and consuming
- 💚 **Health Checks**: Built-in ASP.NET Core health check support
- 💉 **Dependency Injection**: First-class DI support with Microsoft.Extensions.DependencyInjection
- 🔗 **Connection Pooling**: Optimized channel management with automatic recovery
- 📊 **Multiple Queue Types**: Support for Classic, Quorum, Stream, and Lazy queues
- 🗄️ **No EF Core Dependency**: Uses high-performance ADO.NET for database operations

## Installation

```bash
dotnet add package MessagingOverQueue
```

## Quick Start

### 1. Define Your Messages

```csharp
using MessagingOverQueue.Abstractions.Messages;

// Event - can be consumed by multiple subscribers
public class OrderCreatedEvent : Event
{
    public Guid OrderId { get; init; }
    public string CustomerId { get; init; } = string.Empty;
    public decimal TotalAmount { get; init; }
}

// Command - handled by exactly one consumer
public class CreateOrderCommand : Command
{
    public string CustomerId { get; init; } = string.Empty;
    public List<OrderItem> Items { get; init; } = [];
}
```

### 2. Create Message Handlers

```csharp
using MessagingOverQueue.Abstractions.Consuming;

public class OrderCreatedHandler : IMessageHandler<OrderCreatedEvent>
{
    private readonly ILogger<OrderCreatedHandler> _logger;

    public OrderCreatedHandler(ILogger<OrderCreatedHandler> logger)
    {
        _logger = logger;
    }

    public async Task HandleAsync(
        OrderCreatedEvent message, 
        IMessageContext context, 
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Processing order {OrderId}", message.OrderId);
        // Handle the event...
    }
}
```

### 3. Configure Services (Handler-Based Auto-Discovery)

```csharp
using MessagingOverQueue.DependencyInjection;
using MessagingOverQueue.Topology.DependencyInjection;

services.AddRabbitMqMessaging(builder.Configuration)
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderCreatedHandler>());
```

**That's it!** The library automatically:
- ✅ Scans for `IMessageHandler<T>` implementations in your assembly
- ✅ Creates exchanges based on message type (events → topic, commands → direct)
- ✅ Creates queues with service-specific names
- ✅ Sets up bindings with smart routing keys
- ✅ Registers handlers in DI
- ✅ Configures consumers for each handler's queue
- ✅ Configures dead letter queues (optional)

## Handler-Based Topology Discovery

The library's primary auto-discovery mode scans for message handlers rather than message types. This approach is more intuitive because:

1. **Handlers define consumption** - Where messages are processed
2. **Automatic consumer setup** - Each handler gets a consumer automatically
3. **Service isolation** - Different services can handle the same event with their own queues
4. **Less configuration** - No need to manually register handlers or consumers

### Handler Architecture & Registration

MessagingOverQueue uses a sophisticated **handler invoker pattern** to eliminate reflection overhead during message processing.

#### Registration Phase (Startup)

1. **Assembly Scanning**: The `TopologyScanner` discovers all `IMessageHandler<TMessage>` implementations
2. **Handler Registration**: Each handler is registered in the DI container with scoped lifetime
3. **Invoker Creation**: A strongly-typed `HandlerInvoker<TMessage>` is created for each message type
4. **Registry Caching**: Invokers are cached in the `HandlerInvokerRegistry` (ConcurrentDictionary)
5. **Consumer Setup**: A consumer is configured for each handler's queue with appropriate prefetch and concurrency settings

```csharp
// Happens automatically during startup
services.AddRabbitMqMessaging(builder.Configuration)
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderCreatedHandler>());

// Behind the scenes:
// 1. Finds: OrderCreatedHandler : IMessageHandler<OrderCreatedEvent>
// 2. Registers: services.AddScoped<IMessageHandler<OrderCreatedEvent>, OrderCreatedHandler>()
// 3. Creates: var invoker = new HandlerInvoker<OrderCreatedEvent>()
// 4. Caches: registry.Register(invoker)
// 5. Sets up consumer for "order-service.order-created" queue
```

#### Message Processing Phase (Runtime)

1. **Message Received**: Consumer receives message from RabbitMQ
2. **O(1) Lookup**: `HandlerInvokerRegistry.GetInvoker(messageType)` retrieves cached invoker
3. **Scoped Resolution**: Creates DI scope and resolves `IMessageHandler<TMessage>` (your handler)
4. **Strongly-Typed Invocation**: Calls `handler.HandleAsync((TMessage)message, context, ct)` - **no reflection**
5. **Cleanup**: Disposes scope when handler completes

```csharp
// Inside RabbitMqConsumer - simplified
private async Task HandleMessageAsync(ConsumeContext context, CancellationToken ct)
{
    // O(1) dictionary lookup - no reflection
    var invoker = handlerInvokerRegistry.GetInvoker(context.MessageType);
    
    // Create scope and invoke strongly-typed handler
    using var scope = serviceProvider.CreateScope();
    await invoker.InvokeAsync(scope.ServiceProvider, context.Message, context.MessageContext, ct);
}
```

**Performance Benefits:**
- ✅ Reflection used only once per message type at startup
- ✅ O(1) handler lookup via `ConcurrentDictionary`
- ✅ Strongly-typed method calls (no `MethodInfo.Invoke`)
- ✅ Zero allocation per-message (cached invokers)
- ✅ Thread-safe registry with no locking during reads

### Handler Lifetime & Dependency Injection

Handlers are registered with **scoped lifetime**, meaning:
- A new handler instance is created for each message
- Scoped dependencies (like `DbContext`) are automatically managed
- No shared state between concurrent message processing
- Automatic disposal after message handling completes

```csharp
public class OrderCreatedHandler : IMessageHandler<OrderCreatedEvent>
{
    private readonly AppDbContext _context;        // Scoped
    private readonly IEmailService _emailService;  // Can be Scoped, Transient, or Singleton
    
    public OrderCreatedHandler(AppDbContext context, IEmailService emailService)
    {
        _context = context;
        _emailService = emailService;
    }
    
    public async Task HandleAsync(OrderCreatedEvent message, IMessageContext context, CancellationToken ct)
    {
        // Each message gets its own handler instance and DbContext
        var customer = await _context.Customers.FindAsync(message.CustomerId, ct);
        await _emailService.SendOrderConfirmationAsync(customer.Email, message.OrderId);
    }
}
```

### Basic Handler (Convention-Based)

```csharp
public class OrderCreatedHandler : IMessageHandler<OrderCreatedEvent>
{
    public Task HandleAsync(OrderCreatedEvent message, IMessageContext context, CancellationToken ct)
    {
        // Handle the event
        return Task.CompletedTask;
    }
}
```

**Generated Topology:**
- Exchange: `events.order-created` (topic, durable)
- Queue: `{service-name}.order-created` (durable)
- Routing Key: `{category}.order.created`
- Consumer: Auto-registered with default settings

### Handler with Custom Queue Configuration

Use `[ConsumerQueue]` attribute to customize the consumer's queue:

```csharp
using MessagingOverQueue.Topology.Attributes;

[ConsumerQueue(
    Name = "critical-payments",
    QueueType = QueueType.Quorum,
    PrefetchCount = 20,
    MaxConcurrency = 5)]
public class PaymentHandler : IMessageHandler<PaymentProcessedEvent>
{
    public Task HandleAsync(PaymentProcessedEvent message, IMessageContext context, CancellationToken ct)
    {
        return Task.CompletedTask;
    }
}
```

### Multiple Services Handling Same Event

Different services can subscribe to the same events with their own queues:

```csharp
// In Notification Service
services.AddRabbitMqMessaging(config)
    .AddTopology(topology => topology
        .WithServiceName("notification-service")
        .ScanAssemblyContaining<NotifyOnOrderHandler>());
// Queue: notification-service.order-created

// In Analytics Service
services.AddRabbitMqMessaging(config)
    .AddTopology(topology => topology
        .WithServiceName("analytics-service")
        .ScanAssemblyContaining<TrackOrderHandler>());
// Queue: analytics-service.order-created

// Both queues bound to: events.order-created exchange
```

### Consumer Concurrency & Performance Tuning

MessagingOverQueue provides fine-grained control over message consumption performance through the `[ConsumerQueue]` attribute or consumer options.

#### Understanding Consumer Settings

**PrefetchCount**: Number of messages RabbitMQ delivers to the consumer before waiting for acknowledgment
- Higher values = Better throughput (less network roundtrips)
- Lower values = Better load distribution across consumers
- Default: 10

**MaxConcurrency**: Maximum number of messages processed concurrently by this consumer
- Controls parallel handler execution via `SemaphoreSlim`
- Prevents resource exhaustion (e.g., database connection pool)
- Default: 1 (sequential processing)

**ProcessingTimeout**: Maximum time allowed for handler execution
- Set in `ConsumerOptions` (not attribute)
- Automatically cancels long-running handlers
- Default: Configured in options

```csharp
// Low-latency, high-throughput handler
[ConsumerQueue(PrefetchCount = 50, MaxConcurrency = 10)]
public class HighThroughputHandler : IMessageHandler<TelemetryEvent>
{
    public async Task HandleAsync(TelemetryEvent message, IMessageContext context, CancellationToken ct)
    {
        // Process up to 10 messages concurrently
        // RabbitMQ keeps 50 messages buffered
    }
}

// Resource-intensive handler with controlled concurrency
[ConsumerQueue(PrefetchCount = 5, MaxConcurrency = 2)]
public class DatabaseHeavyHandler : IMessageHandler<ReportGeneratedEvent>
{
    private readonly AppDbContext _context;
    
    public async Task HandleAsync(ReportGeneratedEvent message, IMessageContext context, CancellationToken ct)
    {
        // Only 2 concurrent handlers to avoid overwhelming database
        // Only 5 messages prefetched to prevent queue hogging
    }
}

// Sequential processing for order-sensitive messages
[ConsumerQueue(PrefetchCount = 1, MaxConcurrency = 1)]
public class OrderedHandler : IMessageHandler<SequentialEvent>
{
    public async Task HandleAsync(SequentialEvent message, IMessageContext context, CancellationToken ct)
    {
        // Strict sequential processing - one message at a time
    }
}
```

## Configuration Options

### Option A: Handler-Based Auto-Discovery (Recommended)

```csharp
services.AddRabbitMqMessaging(builder.Configuration)
    .AddTopology(topology => topology
        .WithServiceName("my-service")
        .WithDeadLetterEnabled(true)
        .ScanAssemblyContaining<MyHandler>());
```

### Option B: Message Attribute-Based Configuration

Add attributes to message classes for fine-grained control:

```csharp
using MessagingOverQueue.Topology.Attributes;

[Exchange("payments-exchange", Type = ExchangeType.Topic)]
[Queue("payment-processed-queue", QueueType = QueueType.Quorum)]
[RoutingKey("payments.processed")]
[DeadLetter("payments-dlx", QueueName = "payments-failed")]
public class PaymentProcessedEvent : Event
{
    public Guid PaymentId { get; init; }
}
```

### Option C: Fluent API Configuration

```csharp
services.AddRabbitMqMessaging(options => options
    .UseHost("localhost")
    .UsePort(5672)
    .WithCredentials("guest", "guest"))
    .AddTopology(topology => topology
        .AddTopology<PaymentProcessedEvent>(msg => msg
            .WithExchange(ex => ex
                .WithName("payments")
                .AsTopic()
                .Durable())
            .WithQueue(q => q
                .WithName("payment-events")
                .Durable()
                .AsQuorumQueue())
            .WithRoutingKey("payments.processed")
            .WithDeadLetter()));
```

### Option D: Configuration from appsettings.json

```csharp
services.AddRabbitMqMessaging(builder.Configuration);
```

```json
{
  "RabbitMq": {
    "HostName": "localhost",
    "Port": 5672,
    "UserName": "guest",
    "Password": "guest"
  }
}
```

### Option E: .NET Aspire Integration

```csharp
services.AddRabbitMqMessagingFromAspire(builder.Configuration);
```

### Option F: Combined Configuration Sources

```csharp
// appsettings.json provides base config, fluent API overrides
services.AddRabbitMqMessaging(
    builder.Configuration,
    options => options.WithConnectionName("MyApp"));
```

## Publishing Messages

```csharp
using MessagingOverQueue.Abstractions.Publishing;

public class OrderController : ControllerBase
{
    private readonly ICommandSender _commandSender;
    private readonly IEventPublisher _eventPublisher;

    public OrderController(ICommandSender commandSender, IEventPublisher eventPublisher)
    {
        _commandSender = commandSender;
        _eventPublisher = eventPublisher;
    }

    [HttpPost]
    public async Task<IActionResult> CreateOrder(CreateOrderRequest request)
    {
        await _commandSender.SendAsync(new CreateOrderCommand
        {
            CustomerId = request.CustomerId,
            Items = request.Items
        });
        
        return Accepted();
    }

    [HttpPost("{id}/ship")]
    public async Task<IActionResult> ShipOrder(Guid id)
    {
        await _eventPublisher.PublishAsync(new OrderShippedEvent
        {
            OrderId = id,
            ShippedAt = DateTime.UtcNow
        });
        
        return Ok();
    }
}
```

## Attributes Reference

### Message Attributes

| Attribute | Target | Description |
|-----------|--------|-------------|
| `[Exchange]` | Message | Configure exchange name, type, durability |
| `[Queue]` | Message | Configure queue name, type, TTL, max length |
| `[RoutingKey]` | Message | Set the routing key pattern |
| `[DeadLetter]` | Message | Configure dead letter exchange and queue |
| `[Message]` | Message | Control auto-discovery, versioning |
| `[Binding]` | Message | Add multiple routing key bindings |
| `[RetryPolicy]` | Message | Configure retry behavior |

### Handler Attributes

| Attribute | Target | Description |
|-----------|--------|-------------|
| `[ConsumerQueue]` | Handler | Configure consumer queue, prefetch, concurrency |

### ConsumerQueueAttribute Properties

```csharp
[ConsumerQueue(
    Name = "custom-queue-name",      // Override queue name
    QueueType = QueueType.Quorum,    // Classic, Quorum, Stream, Lazy
    Durable = true,                  // Queue durability
    Exclusive = false,               // Exclusive to this connection
    AutoDelete = false,              // Delete when unused
    MessageTtlMs = 86400000,         // Message TTL in milliseconds
    MaxLength = 10000,               // Max messages in queue
    MaxLengthBytes = 1073741824,     // Max queue size in bytes
    PrefetchCount = 10,              // Consumer prefetch count
    MaxConcurrency = 5               // Max concurrent handlers
)]
public class MyHandler : IMessageHandler<MyEvent> { }
```

## Naming Conventions

### Default Naming

| Element | Event | Command |
|---------|-------|---------|
| Exchange | `events.{message-name}` | `commands.{message-name}` |
| Queue | `{service-name}.{message-name}` | `{message-name}` |
| Routing Key | `{category}.{message-name}` | `{message-name}` |
| Dead Letter Exchange | `dlx.{queue-name}` | `dlx.{queue-name}` |
| Dead Letter Queue | `{queue-name}.dlq` | `{queue-name}.dlq` |

### Customize Naming

```csharp
.AddTopology(topology => topology
    .WithServiceName("my-service")
    .ConfigureNaming(naming =>
    {
        naming.UseLowerCase = true;
        naming.EventExchangePrefix = "events.";
        naming.CommandExchangePrefix = "commands.";
        naming.DeadLetterExchangePrefix = "dlx.";
        naming.DeadLetterQueueSuffix = "dlq";
        naming.QueueSeparator = ".";
    }));
```

## Outbox Pattern (Provider-Based)

Ensure messages are published reliably within database transactions. The outbox pattern uses a **pluggable provider architecture** - no Entity Framework Core dependency required.

### Supported Providers

| Provider | Package | Status |
|----------|---------|--------|
| SQL Server | Built-in | ✅ Available |
| PostgreSQL | Coming soon | 🔜 Planned |
| MySQL | Coming soon | 🔜 Planned |
| In-Memory | Built-in (testing) | ✅ Available |

### 1. Configure the Outbox with SQL Server

```csharp
using MessagingOverQueue.DependencyInjection;
using MessagingOverQueue.Persistence.DependencyInjection;

services.AddRabbitMqMessaging(builder.Configuration)
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderHandler>())
    .AddOutboxPattern(options =>
    {
        options.ProcessingInterval = TimeSpan.FromSeconds(5);
        options.BatchSize = 100;
        options.AutoCreateSchema = true;  // Auto-create table on startup
    })
    .UseSqlServer(connectionString, store =>
    {
        store.TableName = "MessageStore";     // Custom table name
        store.Schema = "messaging";           // Custom schema (optional)
        store.AutoCreateSchema = true;        // Create table if not exists
        store.CommandTimeoutSeconds = 30;     // Command timeout
    });
```

### 2. Use Transactional Publishing

```csharp
using MessagingOverQueue.Persistence;

public class OrderService
{
    private readonly OutboxPublisher _outboxPublisher;

    public OrderService(OutboxPublisher outboxPublisher)
    {
        _outboxPublisher = outboxPublisher;
    }

    public async Task CreateOrderAsync(CreateOrderCommand command)
    {
        // Messages are stored in the outbox and processed by background service
        await _outboxPublisher.PublishAsync(new OrderCreatedEvent
        {
            OrderId = Guid.NewGuid(),
            CustomerId = command.CustomerId
        });
    }
}
```

### 3. Outbox Configuration Options

```csharp
.AddOutboxPattern(options =>
{
    options.Enabled = true;                              // Enable/disable outbox
    options.ProcessingInterval = TimeSpan.FromSeconds(5); // How often to check for pending messages
    options.BatchSize = 100;                             // Messages per batch
    options.MaxRetryAttempts = 5;                        // Max retries before marking as failed
    options.LockDuration = TimeSpan.FromMinutes(5);      // Lock timeout for processing
    options.AutoCleanup = true;                          // Auto-delete old messages
    options.RetentionPeriod = TimeSpan.FromDays(1);      // How long to keep processed messages
    options.CleanupInterval = TimeSpan.FromHours(1);     // How often to run cleanup
    options.AutoCreateSchema = true;                     // Create database table on startup
});
```

### Message Store Schema

The provider creates a unified `MessageStore` table for both outbox and inbox entries:

```sql
CREATE TABLE [MessageStore] (
    [Id] UNIQUEIDENTIFIER NOT NULL,
    [Direction] INT NOT NULL,           -- 0 = Outbox, 1 = Inbox
    [MessageType] NVARCHAR(500) NOT NULL,
    [Payload] VARBINARY(MAX) NULL,
    [ExchangeName] NVARCHAR(256) NULL,
    [RoutingKey] NVARCHAR(256) NULL,
    [Headers] NVARCHAR(MAX) NULL,
    [HandlerType] NVARCHAR(500) NULL,   -- For inbox idempotency
    [CreatedAt] DATETIME2 NOT NULL,
    [ProcessedAt] DATETIME2 NULL,
    [Status] INT NOT NULL,              -- 0=Pending, 1=Processing, 2=Published, 3=Failed
    [RetryCount] INT NOT NULL DEFAULT 0,
    [LastError] NVARCHAR(4000) NULL,
    [LockToken] NVARCHAR(100) NULL,
    [LockExpiresAt] DATETIME2 NULL,
    [CorrelationId] NVARCHAR(100) NULL,
    
    PRIMARY KEY CLUSTERED ([Id], [Direction], [HandlerType])
);
```

### Implementing Custom Providers

Create your own provider by implementing `IMessageStoreProvider`:

```csharp
public class PostgreSqlMessageStoreProvider : IMessageStoreProvider
{
    public Task AddAsync(MessageStoreEntry entry, CancellationToken ct = default) { ... }
    public Task<IReadOnlyList<MessageStoreEntry>> AcquireOutboxLockAsync(int batchSize, TimeSpan lockDuration, CancellationToken ct = default) { ... }
    public Task MarkAsPublishedAsync(Guid messageId, CancellationToken ct = default) { ... }
    public Task MarkAsFailedAsync(Guid messageId, string error, CancellationToken ct = default) { ... }
    public Task<bool> ExistsInboxEntryAsync(Guid messageId, string handlerType, CancellationToken ct = default) { ... }
    public Task CleanupAsync(MessageDirection direction, TimeSpan retentionPeriod, CancellationToken ct = default) { ... }
    public Task EnsureSchemaAsync(CancellationToken ct = default) { ... }
    // ... other methods
}
```

Register your custom provider:

```csharp
services.AddRabbitMqMessaging(config)
    .AddOutboxPattern()
    .Services.AddSingleton<IMessageStoreProvider, PostgreSqlMessageStoreProvider>();
```

## Resilience Configuration

```csharp
services.AddRabbitMqMessaging(config)
    .AddTopology(topology => topology
        .WithServiceName("my-service")
        .ScanAssemblyContaining<MyHandler>())
    .ConfigureRetry(retry =>
    {
        retry.MaxRetryAttempts = 5;
        retry.InitialDelay = TimeSpan.FromSeconds(1);
        retry.MaxDelay = TimeSpan.FromMinutes(5);
        retry.UseExponentialBackoff = true;
    })
    .AddCircuitBreaker(cb =>
    {
        cb.FailureRateThreshold = 0.5;
        cb.DurationOfBreak = TimeSpan.FromSeconds(30);
    });
```

## Queue Types

```csharp
// High Availability - Quorum Queue
[ConsumerQueue(QueueType = QueueType.Quorum)]
public class CriticalHandler : IMessageHandler<CriticalEvent> { }

// High Throughput - Stream Queue (no DLX support)
[ConsumerQueue(QueueType = QueueType.Stream)]
public class TelemetryHandler : IMessageHandler<TelemetryEvent> { }

// Large Queues - Lazy Queue
[ConsumerQueue(QueueType = QueueType.Lazy, MaxLength = 1000000)]
public class BulkHandler : IMessageHandler<BulkEvent> { }
```

## Health Checks

```csharp
services.AddRabbitMqMessaging(config)
    .AddTopology(...)
    .AddHealthChecks();

app.MapHealthChecks("/health");
```

## Message Context

Access message metadata in handlers:

```csharp
public async Task HandleAsync(MyEvent message, IMessageContext context, CancellationToken ct)
{
    Console.WriteLine($"Message ID: {context.MessageId}");
    Console.WriteLine($"Correlation ID: {context.CorrelationId}");
    Console.WriteLine($"Queue: {context.QueueName}");
    Console.WriteLine($"Delivery Count: {context.DeliveryCount}");
    Console.WriteLine($"Received At: {context.ReceivedAt}");
    
    var customHeader = context.Headers["x-custom-header"];
}
```

## Handler Registration Methods

### Automatic Registration (Recommended)

Scans assemblies and automatically registers all handlers:

```csharp
services.AddRabbitMqMessaging(builder.Configuration)
    .AddTopology(topology => topology
        .WithServiceName("my-service")
        .ScanAssemblyContaining<OrderCreatedHandler>());

// Automatically registers:
// - IMessageHandler<OrderCreatedEvent> → OrderCreatedHandler (scoped)
// - HandlerInvoker<OrderCreatedEvent> in registry
// - Consumer for queue "my-service.order-created"
// - Message type for serialization
```

### Manual Handler Registration

For fine-grained control, register handlers explicitly:

```csharp
services.AddRabbitMqMessaging(builder.Configuration)
    .AddHandler<OrderCreatedHandler, OrderCreatedEvent>()
    .AddHandler<PaymentProcessedHandler, PaymentProcessedEvent>()
    .AddConsumer("order-events", opt => 
    {
        opt.PrefetchCount = 20;
        opt.MaxConcurrency = 5;
    });
```

### Multiple Handlers for Same Message

Register multiple handlers for a single message type:

```csharp
services.AddRabbitMqMessaging(builder.Configuration)
    .AddHandler<EmailNotificationHandler, OrderCreatedEvent>()
    .AddHandler<AuditLoggingHandler, OrderCreatedEvent>()
    .AddHandler<AnalyticsTrackingHandler, OrderCreatedEvent>();

// When OrderCreatedEvent is received:
// 1. HandlerInvoker<OrderCreatedEvent> resolves ALL handlers from DI
// 2. Executes them sequentially in registration order
// 3. All handlers must succeed for message acknowledgment
```

## Complete Registration Example

```csharp
services.AddRabbitMqMessaging(builder.Configuration, options => options
    .WithConnectionName("MyApp")
    .WithChannelPoolSize(20))
    
    // Handler-based auto-discovery - this does everything!
    .AddTopology(topology => topology
        .WithServiceName("my-service")
        .WithDeadLetterEnabled(true)
        .ScanAssemblyContaining<OrderCreatedHandler>()
        .ConfigureProvider(provider =>
        {
            provider.DefaultDurable = true;
            provider.EnableDeadLetterByDefault = true;
        }))
    
    // Add outbox pattern with SQL Server
    .AddOutboxPattern(outbox =>
    {
        outbox.ProcessingInterval = TimeSpan.FromSeconds(5);
        outbox.BatchSize = 100;
        outbox.AutoCreateSchema = true;
    })
    .UseSqlServer(connectionString, store =>
    {
        store.TableName = "MessageStore";
        store.Schema = "messaging";
    })
    
    // Configure resilience
    .ConfigureRetry(retry => 
    {
        retry.MaxRetryAttempts = 5;
        retry.UseExponentialBackoff = true;
    })
    .AddCircuitBreaker(cb =>
    {
        cb.FailureRateThreshold = 0.5;
        cb.DurationOfBreak = TimeSpan.FromSeconds(30);
    })
    
    // Add health checks
    .AddHealthChecks();
```

## Project Structure

```
MessagingOverQueue/
├── src/
│   ├── Abstractions/
│   │   ├── Consuming/          # IMessageHandler, IMessageContext, IMessageConsumer
│   │   ├── Messages/           # IMessage, IEvent, ICommand, IQuery, MessageBase
│   │   ├── Publishing/         # IMessagePublisher, IEventPublisher, ICommandSender
│   │   └── Serialization/      # IMessageSerializer, IMessageTypeResolver
│   ├── Configuration/
│   │   ├── Builders/           # RabbitMqOptionsBuilder, fluent builders
│   │   ├── Options/            # RabbitMqOptions, ConsumerOptions, RetryOptions
│   │   └── Sources/            # Configuration sources (Aspire, AppSettings, Fluent)
│   ├── Connection/             # IRabbitMqConnectionPool, channel management
│   ├── Consuming/
│   │   ├── Handlers/           # HandlerInvokerRegistry, HandlerInvokerFactory
│   │   └── Middleware/         # ConsumePipeline, DeserializationMiddleware
│   ├── DependencyInjection/    # ServiceCollectionExtensions, IMessagingBuilder
│   ├── HealthChecks/           # RabbitMqHealthCheck
│   ├── Hosting/                # ConsumerHostedService, RabbitMqHostedService
│   ├── Persistence/
│   │   ├── DependencyInjection/ # MessageStoreProviderExtensions, IOutboxBuilder
│   │   ├── Entities/           # MessageStoreEntry (unified outbox/inbox)
│   │   ├── Providers/          # IMessageStoreProvider, MessageStoreOptions
│   │   │   └── SqlServer/      # SqlServerMessageStoreProvider
│   │   └── Repositories/       # IOutboxRepository, IInboxRepository
│   ├── Publishing/
│   │   └── Middleware/         # PublishPipeline, SerializationMiddleware
│   ├── Resilience/
│   │   └── CircuitBreaker/     # ICircuitBreaker, PollyCircuitBreaker
│   └── Topology/
│       ├── Abstractions/       # ITopologyScanner, ITopologyRegistry, ITopologyProvider
│       ├── Attributes/         # ConsumerQueueAttribute, ExchangeAttribute, etc.
│       ├── Builders/           # TopologyBuilder, MessageTopologyBuilder
│       ├── Conventions/        # DefaultTopologyNamingConvention
│       └── DependencyInjection/# TopologyServiceCollectionExtensions
└── Examples/                   # Configuration and topology examples
```

## Key Benefits

✅ **Zero-Config Handlers**: Handlers are automatically registered and connected to consumers  
✅ **Reflection-Free Dispatch**: Handler invoker pattern eliminates per-message reflection overhead  
✅ **O(1) Handler Lookup**: ConcurrentDictionary-based registry for instant handler resolution  
✅ **Service Isolation**: Each service gets its own queue for shared events  
✅ **Type Safety**: Strongly-typed messages and handlers with compile-time verification  
✅ **Scoped DI**: Automatic scope management for each message handler  
✅ **Multiple Handlers**: Native support for multiple handlers per message type  
✅ **Concurrency Control**: Fine-grained control via SemaphoreSlim and prefetch settings  
✅ **Provider-Based Persistence**: Pluggable database providers without EF Core dependency  
✅ **High-Performance ADO.NET**: Direct database access with optimized queries  
✅ **Unified Message Store**: Single table for both outbox and inbox (idempotency)  
✅ **Atomic Locking**: SQL Server's OUTPUT clause for race-condition-free message acquisition  
✅ **Auto Schema Creation**: Database tables created automatically on startup  
✅ **Resilience**: Built-in retry, circuit breaker, and dead letter handling  
✅ **Flexibility**: Mix auto-discovery with manual configuration  
✅ **Production Ready**: Health checks, monitoring, and enterprise patterns  
✅ **High Performance**: Connection pooling, optimized serialization, minimal allocations  

## Migration from EF Core-based Outbox

If you're migrating from the previous EF Core-based outbox pattern:

### Before (EF Core)
```csharp
// Old approach - required DbContext
services.AddRabbitMqMessaging(config)
    .AddOutboxPattern<AppDbContext>(options => { });
```

### After (Provider-Based)
```csharp
// New approach - provider-based, no EF Core required
services.AddRabbitMqMessaging(config)
    .AddOutboxPattern(options => { })
    .UseSqlServer(connectionString);
```

### Data Migration

The new unified `MessageStore` table replaces the separate `OutboxMessages` and `InboxMessages` tables. A migration script is recommended for production environments:

```sql
-- Example migration script (adjust as needed)
INSERT INTO MessageStore (Id, Direction, MessageType, Payload, ExchangeName, RoutingKey, 
    Headers, CreatedAt, ProcessedAt, Status, RetryCount, LastError, LockToken, LockExpiresAt, CorrelationId)
SELECT Id, 0 as Direction, MessageType, Payload, ExchangeName, RoutingKey,
    Headers, CreatedAt, ProcessedAt, Status, RetryCount, LastError, LockToken, LockExpiresAt, CorrelationId
FROM OutboxMessages;

INSERT INTO MessageStore (Id, Direction, MessageType, HandlerType, CreatedAt, ProcessedAt, Status, CorrelationId)
SELECT Id, 1 as Direction, MessageType, HandlerType, ProcessedAt, ProcessedAt, 2, CorrelationId
FROM InboxMessages;
```

## Documentation

- **[README.md](README.md)** - This file - Quick start and usage guide
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Detailed architecture and design patterns
- **[Examples/](Examples/)** - Sample code and configuration examples

## Requirements

- .NET 10 or later
- RabbitMQ 3.8+ (for quorum queues and streams)
- SQL Server 2016+ (for SQL Server provider)

## License

Apache 2.0

## Contributing

Contributions are welcome! Please visit our [GitHub repository](https://github.com/donakunn/Donakunn.MessagingOverQueue) to:
- Report issues
- Submit pull requests
- Request features
- View the source code

