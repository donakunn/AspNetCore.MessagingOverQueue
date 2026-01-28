# MessagingOverQueue - Redis Streams Messaging Library for .NET

[![GitHub Repository](https://img.shields.io/badge/GitHub-donakunn%2FDonakunn.MessagingOverQueue-blue?logo=github)](https://github.com/donakunn/Donakunn.MessagingOverQueue)
[![.NET 10](https://img.shields.io/badge/.NET-10-purple)](https://dotnet.microsoft.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green)](LICENSE)

A robust, high-performance asynchronous messaging library for .NET 10 with **Redis Streams**, automatic handler-based topology discovery, and SOLID design principles.


## Introduction

**MessagingOverQueue** is a production-ready messaging library designed to eliminate boilerplate code and streamline message-driven architecture in .NET applications. Built with modern .NET best practices, it provides a developer-friendly abstraction over Redis Streams while maintaining full control and flexibility.

### Why MessagingOverQueue?

Traditional messaging integration requires significant boilerplate: manual stream and consumer group declarations, consumer setup, handler registration, and serialization plumbing. MessagingOverQueue eliminates this complexity through **intelligent handler-based auto-discovery** - simply implement `IMessageHandler<T>`, and the library automatically:

- **Discovers your handlers** at startup via assembly scanning
- **Creates messaging topology** (streams, consumer groups) based on conventions or attributes
- **Registers handlers in DI** with scoped lifetime management
- **Sets up consumers** with optimized concurrency and batch settings
- **Dispatches messages** using reflection-free, strongly-typed handler invocation
- **Manages connections** with automatic recovery

### Architecture Highlights

**Provider Abstraction**: The core library is provider-agnostic with clean interfaces:
- `IInternalConsumer` - Message consumption
- `IInternalPublisher` - Message publishing
- `ITopologyDeclarer` - Topology declaration
- `IHealthCheck` - Health checks

**Reflection-Free Handler Dispatch**: Unlike traditional approaches that use reflection for every message, MessagingOverQueue employs a **handler invoker registry** pattern. Generic `HandlerInvoker<TMessage>` instances are created once at startup and cached in a `ConcurrentDictionary`, providing O(1) lookup and zero reflection overhead during message processing.

**Middleware Pipeline**: Extensible middleware architecture for both publishing and consuming, enabling cross-cutting concerns like logging, serialization, validation, and enrichment.

**Topology Management**: Supports convention-based auto-discovery, attribute-based configuration, fluent API, or hybrid approaches for maximum flexibility.

**Provider-Based Persistence**: Pluggable database provider architecture for the Outbox pattern. Choose your database (SQL Server, PostgreSQL, etc.) without being locked into Entity Framework Core.

### Target Scenarios

- **Microservices Communication**: Event-driven architectures, service-to-service messaging, CQRS implementations
- **Background Processing**: Asynchronous job queues, long-running tasks, scheduled workflows
- **Event Sourcing**: Publishing domain events with reliable delivery guarantees
- **Integration Patterns**: Message routing, pub/sub
- **High-Throughput Systems**: Optimized for concurrent message processing with configurable batch size and parallelism

---

## Features

- **Handler-Based Auto-Discovery**: Automatically configure topology by scanning for message handlers - streams and consumer groups are set up automatically
- **Reflection-Free Dispatch**: Handler invoker registry eliminates reflection overhead during message processing
- **Clean Abstractions**: Simple interfaces for publishing and consuming messages (`ICommand`, `IEvent`, `IQuery`)
- **Flexible Configuration**: Multiple configuration sources - Fluent API, appsettings.json, or custom sources
- **Provider-Based Outbox Pattern**: Reliable message delivery with pluggable database providers (SQL Server, with extensibility for others)
- **Resilience**: Built-in retry policies, circuit breakers, and dead letter handling
- **Middleware Pipeline**: Extensible pipeline for both publishing and consuming
- **Health Checks**: Built-in ASP.NET Core health check support
- **Dependency Injection**: First-class DI support with Microsoft.Extensions.DependencyInjection
- **Consumer Groups**: Full support for Redis Streams consumer groups with automatic message claiming
- **No EF Core Dependency**: Uses high-performance ADO.NET for database operations

## Installation

```bash
dotnet add package Donakunn.MessagingOverQueue.RedisStreams
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

### 3. Configure Services

```csharp
using Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;

services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379")
    .WithStreamPrefix("myapp")
    .ConfigureConsumer(consumer => consumer
        .WithBatchSize(100)
        .WithMaxConcurrency(10))
    .WithTimeBasedRetention(TimeSpan.FromDays(7))
    .WithDeadLetterPerConsumerGroup()
    .WithMaxDeliveryAttempts(5))
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderCreatedHandler>());
```

**That's it!** The library automatically:
- Scans for `IMessageHandler<T>` implementations in your assembly
- Creates streams based on message type
- Creates consumer groups with service-specific names
- Registers handlers in DI
- Configures consumers for each handler's stream
- Configures dead letter streams (optional)

## Handler-Based Topology Discovery

The library's primary auto-discovery mode scans for message handlers rather than message types. This approach is more intuitive because:

1. **Handlers define consumption** - Where messages are processed
2. **Automatic consumer setup** - Each handler gets a consumer automatically
3. **Service isolation** - Different services can handle the same event with their own consumer groups
4. **Less configuration** - No need to manually register handlers or consumers

### Handler Architecture & Registration

MessagingOverQueue uses a sophisticated **handler invoker pattern** to eliminate reflection overhead during message processing.

#### Registration Phase (Startup)

1. **Assembly Scanning**: The `TopologyScanner` discovers all `IMessageHandler<TMessage>` implementations
2. **Handler Registration**: Each handler is registered in the DI container with scoped lifetime
3. **Invoker Creation**: A strongly-typed `HandlerInvoker<TMessage>` is created for each message type
4. **Registry Caching**: Invokers are cached in the `HandlerInvokerRegistry` (ConcurrentDictionary)
5. **Consumer Setup**: A consumer is configured for each handler's stream with appropriate batch and concurrency settings

```csharp
// Happens automatically during startup
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379"))
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderCreatedHandler>());

// Behind the scenes:
// 1. Finds: OrderCreatedHandler : IMessageHandler<OrderCreatedEvent>
// 2. Registers: services.AddScoped<IMessageHandler<OrderCreatedEvent>, OrderCreatedHandler>()
// 3. Creates: var invoker = new HandlerInvoker<OrderCreatedEvent>()
// 4. Caches: registry.Register(invoker)
// 5. Sets up consumer group for "order-service" on the stream
```

#### Message Processing Phase (Runtime)

1. **Message Received**: Consumer receives message from Redis stream via XREADGROUP
2. **Middleware Pipeline**: `ConsumePipeline` executes middleware chain (circuit breaker, retry, logging, deserialization)
3. **O(1) Lookup**: `HandlerInvokerRegistry.GetInvoker(messageType)` retrieves cached invoker
4. **Scoped Resolution**: Creates DI scope and resolves `IMessageHandler<TMessage>` (your handler)
5. **Strongly-Typed Invocation**: Calls `handler.HandleAsync((TMessage)message, context, ct)` - **no reflection**
6. **Acknowledgment**: Message is acknowledged via XACK
7. **Cleanup**: Disposes scope when handler completes

```csharp
// Inside ConsumerHostedService - simplified
private async Task HandleMessageAsync(ConsumeContext context, CancellationToken ct)
{
    using var scope = _serviceProvider.CreateScope();

    // Execute middleware pipeline (logging, deserialization, etc.)
    var middlewares = scope.ServiceProvider.GetServices<IConsumeMiddleware>();
    var pipeline = new ConsumePipeline(middlewares, async (ctx, ct) =>
    {
        // O(1) dictionary lookup - no reflection
        var invoker = handlerInvokerRegistry.GetInvoker(ctx.MessageType);

        // Invoke strongly-typed handler
        await invoker.InvokeAsync(scope.ServiceProvider, ctx.Message, ctx.MessageContext, ct);
    });

    await pipeline.ExecuteAsync(context, ct);
}
```

**Performance Benefits:**
- Reflection used only once per message type at startup
- O(1) handler lookup via `ConcurrentDictionary`
- Strongly-typed method calls (no `MethodInfo.Invoke`)
- Zero allocation per-message (cached invokers)
- Thread-safe registry with no locking during reads

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
- Stream: `{prefix}:events:order-created`
- Consumer Group: `{service-name}`
- Consumer: Auto-registered with default settings

### Handler with Custom Consumer Group

Use `[RedisConsumerGroup]` attribute to customize the consumer group:

```csharp
using Donakunn.MessagingOverQueue.Topology.Attributes;

[RedisConsumerGroup("custom-processing-group")]
public class PaymentHandler : IMessageHandler<PaymentProcessedEvent>
{
    public Task HandleAsync(PaymentProcessedEvent message, IMessageContext context, CancellationToken ct)
    {
        return Task.CompletedTask;
    }
}
```

### Multiple Services Handling Same Event

Different services can subscribe to the same events with their own consumer groups:

```csharp
// In Notification Service
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379"))
    .AddTopology(topology => topology
        .WithServiceName("notification-service")
        .ScanAssemblyContaining<NotifyOnOrderHandler>());
// Consumer Group: notification-service

// In Analytics Service
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379"))
    .AddTopology(topology => topology
        .WithServiceName("analytics-service")
        .ScanAssemblyContaining<TrackOrderHandler>());
// Consumer Group: analytics-service

// Both consumer groups read from the same stream
```

### Consumer Concurrency & Performance Tuning

MessagingOverQueue provides fine-grained control over message consumption performance through configuration options.

#### Understanding Consumer Settings

**BatchSize**: Number of messages fetched per XREADGROUP call
- Higher values = Better throughput (fewer Redis roundtrips)
- Lower values = Lower latency for individual messages
- Default: 100

**MaxConcurrency**: Maximum number of messages processed concurrently by this consumer
- Controls parallel handler execution via `SemaphoreSlim`
- Prevents resource exhaustion (e.g., database connection pool)
- Default: 10

**ClaimIdleTime**: Time after which idle messages are automatically reclaimed via XAUTOCLAIM
- Handles failed consumers by redistributing unacknowledged messages
- Default: 5 minutes

```csharp
// High-throughput configuration
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379")
    .ConfigureConsumer(consumer => consumer
        .WithBatchSize(500)
        .WithMaxConcurrency(20)));

// Resource-intensive handler with controlled concurrency
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379")
    .ConfigureConsumer(consumer => consumer
        .WithBatchSize(10)
        .WithMaxConcurrency(2)));

// Sequential processing for order-sensitive messages
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379")
    .ConfigureConsumer(consumer => consumer
        .WithBatchSize(1)
        .WithMaxConcurrency(1)));
```

## Idempotency

**Handlers MUST be idempotent.** The library provides at-least-once delivery semantics, meaning the same message may be delivered multiple times in edge cases:

- Network failures during acknowledgment
- Process restarts/crashes during message handling
- Consumer disconnects before XACK is sent
- XAUTOCLAIM redistributes unacknowledged messages

### Built-in Idempotency Support

When the outbox pattern is enabled, the library provides automatic idempotency tracking via the inbox pattern:

```csharp
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379"))
    .AddOutboxPattern(options => { })
    .UseSqlServer(connectionString);
```

Each message + handler combination is tracked in the `MessageStore` table. If a duplicate message arrives for the same handler, it's automatically skipped.

### Manual Idempotency

For handlers that don't use the outbox pattern, implement idempotency in your handler logic:

```csharp
public class OrderCreatedHandler : IMessageHandler<OrderCreatedEvent>
{
    private readonly AppDbContext _context;

    public async Task HandleAsync(OrderCreatedEvent message, IMessageContext context, CancellationToken ct)
    {
        // Check if already processed
        if (await _context.ProcessedOrders.AnyAsync(p => p.OrderId == message.OrderId, ct))
            return;

        // Process the order
        await _context.Orders.AddAsync(new Order { Id = message.OrderId }, ct);
        await _context.ProcessedOrders.AddAsync(new ProcessedOrder { OrderId = message.OrderId }, ct);
        await _context.SaveChangesAsync(ct);
    }
}
```

---

## Middleware Pipeline

The library uses an extensible middleware pipeline for both publishing and consuming messages.

### Consume Pipeline Order

When a message is received, it flows through middlewares in this order:

```
Message Received (XREADGROUP)
    ↓
CircuitBreaker Middleware (if configured)
    ↓
Retry Middleware (if configured)
    ↓
Timeout Middleware (if configured)
    ↓
Logging Middleware
    ↓
Idempotency Middleware (if outbox enabled)
    ↓
Deserialization Middleware
    ↓
Handler Invocation
    ↓
Acknowledgment (XACK)
```

### Built-in Consume Middlewares

| Middleware | Order | Purpose |
|------------|-------|---------|
| `CircuitBreakerMiddleware` | 100 | Prevents cascade failures |
| `RetryMiddleware` | 200 | Automatic retry with backoff |
| `TimeoutMiddleware` | 300 | Handler execution timeout |
| `ConsumeLoggingMiddleware` | 400 | Request/response logging |
| `IdempotencyMiddleware` | 500 | Duplicate message detection |
| `DeserializationMiddleware` | 600 | JSON → message object |

### Custom Middleware

Create custom middleware by implementing `IConsumeMiddleware`:

```csharp
public class MetricsMiddleware : IOrderedConsumeMiddleware
{
    public int Order => 350; // After timeout, before logging

    public async Task InvokeAsync(
        ConsumeContext context,
        Func<ConsumeContext, CancellationToken, Task> next,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            await next(context, cancellationToken);
            RecordSuccess(context.MessageType, stopwatch.Elapsed);
        }
        catch (Exception ex)
        {
            RecordFailure(context.MessageType, stopwatch.Elapsed, ex);
            throw;
        }
    }
}
```

Register in DI:

```csharp
services.AddSingleton<IConsumeMiddleware, MetricsMiddleware>();
```

---

## Configuration Options

### Option A: Fluent API (Recommended)

```csharp
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379")
    .WithStreamPrefix("myapp")
    .ConfigureConsumer(consumer => consumer
        .WithBatchSize(100)
        .WithMaxConcurrency(10))
    .WithTimeBasedRetention(TimeSpan.FromDays(7))
    .WithDeadLetterPerConsumerGroup()
    .WithMaxDeliveryAttempts(5))
    .AddTopology(topology => topology
        .WithServiceName("my-service")
        .ScanAssemblyContaining<MyHandler>());
```

### Option B: Configuration from appsettings.json

```csharp
services.AddRedisStreamsMessaging(builder.Configuration)
    .AddTopology(topology => topology
        .WithServiceName("my-service")
        .ScanAssemblyContaining<MyHandler>());
```

```json
{
  "RedisStreams": {
    "ConnectionString": "localhost:6379",
    "Password": "secret",
    "Database": 0,
    "StreamPrefix": "myapp",
    "ConnectionTimeout": "00:00:30",
    "ConsumerOptions": {
      "BatchSize": 100,
      "MaxConcurrency": 10,
      "ClaimIdleTime": "00:05:00",
      "BlockingTimeout": "00:00:05"
    },
    "RetentionStrategy": "TimeBased",
    "RetentionPeriod": "7.00:00:00",
    "DeadLetterStrategy": "PerConsumerGroup",
    "MaxDeliveryAttempts": 5
  }
}
```

### Option C: Combined Configuration Sources

```csharp
// appsettings.json provides base config, fluent API overrides
services.AddRedisStreamsMessaging(
    builder.Configuration,
    options => options.WithStreamPrefix("override-prefix"));
```

---

## Redis Streams Configuration

### Basic Configuration

```csharp
using Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection;

services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379,password=secret")
    .WithStreamPrefix("myapp"))
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderCreatedHandler>());
```

### Advanced Configuration

```csharp
services.AddRedisStreamsMessaging(options => options
    // Connection
    .UseConnectionString("localhost:6379")
    .UseDatabase(0)
    .WithConnectionTimeout(TimeSpan.FromSeconds(30))
    .WithStreamPrefix("myapp")
    
    // Consumer settings
    .ConfigureConsumer(consumer => consumer
        .WithBatchSize(100)              // Messages per XREADGROUP call
        .WithMaxConcurrency(10)          // Parallel message processing
        .WithClaimIdleTime(TimeSpan.FromMinutes(5))  // XAUTOCLAIM threshold
        .WithBlockingTimeout(TimeSpan.FromSeconds(5)))
    
    // Retention strategy (choose one)
    .WithTimeBasedRetention(TimeSpan.FromDays(7))    // MINID trimming
    // OR
    .WithCountBasedRetention(100_000)                // MAXLEN trimming
    
    // Dead letter handling
    .WithDeadLetterPerConsumerGroup()   // DLQ per consumer group
    .WithMaxDeliveryAttempts(5))        // Attempts before DLQ
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderCreatedHandler>());
```

### Configuration from appsettings.json

```csharp
services.AddRedisStreamsMessaging(builder.Configuration);
```

```json
{
  "RedisStreams": {
    "ConnectionString": "localhost:6379",
    "Password": "secret",
    "Database": 0,
    "StreamPrefix": "myapp",
    "ConnectionTimeout": "00:00:30",
    "ConsumerOptions": {
      "BatchSize": 100,
      "MaxConcurrency": 10,
      "ClaimIdleTime": "00:05:00",
      "BlockingTimeout": "00:00:05"
    },
    "RetentionStrategy": "TimeBased",
    "RetentionPeriod": "7.00:00:00",
    "DeadLetterStrategy": "PerConsumerGroup",
    "MaxDeliveryAttempts": 5
  }
}
```

### Redis Streams Concepts

| Concept | Description |
|---------|-------------|
| **Stream** | Append-only log structure (like Kafka topics) |
| **Consumer Group** | Named group that tracks message delivery state |
| **Consumer** | Instance within a group that processes messages |
| **Pending Entries List (PEL)** | Messages delivered but not acknowledged |
| **XAUTOCLAIM** | Automatically reclaim idle messages from failed consumers |

### Consumer Group Naming

By default, consumer groups are named after the service name configured in topology. Override with the `[RedisConsumerGroup]` attribute:

```csharp
using Donakunn.MessagingOverQueue.Topology.Attributes;

[RedisConsumerGroup("custom-processing-group")]
public class OrderCreatedHandler : IMessageHandler<OrderCreatedEvent>
{
    // ...
}
```

### Dead Letter Handling

Redis Streams don't have native dead letter support. This library implements DLQ by:

1. Tracking delivery count via `XPENDING`
2. Moving messages to a DLQ stream after `MaxDeliveryAttempts`
3. Creating DLQ streams named: `{prefix}:{stream-name}:dlq` or `{prefix}:{stream-name}:{group}:dlq`

### Health Checks

```csharp
services.AddRedisStreamsMessaging(options => { /* ... */ })
    .AddHealthChecks(
        name: "redis-streams",
        failureStatus: HealthStatus.Unhealthy,
        tags: ["ready", "messaging"]);
```

---

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

### Handler Attributes

| Attribute | Target | Description |
|-----------|--------|-------------|
| `[RedisConsumerGroup]` | Handler | Override consumer group name |
| `[Message]` | Message | Control auto-discovery, versioning |
| `[RetryPolicy]` | Message | Configure retry behavior |

### RedisConsumerGroupAttribute

```csharp
[RedisConsumerGroup("custom-group-name")]
public class MyHandler : IMessageHandler<MyEvent> { }
```

## Naming Conventions

### Default Naming

| Element | Event | Command |
|---------|-------|---------|
| Stream | `{prefix}:events:{message-name}` | `{prefix}:commands:{message-name}` |
| Consumer Group | `{service-name}` | `{service-name}` |
| Dead Letter Stream | `{stream-name}:dlq` | `{stream-name}:dlq` |

### Customize Naming

```csharp
.AddTopology(topology => topology
    .WithServiceName("my-service")
    .ConfigureNaming(naming =>
    {
        naming.UseLowerCase = true;
        naming.StreamPrefix = "myapp";
        naming.DeadLetterSuffix = "dlq";
    }));
```

## Outbox Pattern (Provider-Based)

Ensure messages are published reliably within database transactions. The outbox pattern uses a **pluggable provider architecture** - no Entity Framework Core dependency required.

### Supported Providers

| Provider | Package | Status |
|----------|---------|--------|
| SQL Server | Built-in | Available |
| PostgreSQL | Coming soon | Planned |
| MySQL | Coming soon | Planned |
| In-Memory | Built-in (testing) | Available |

### 1. Configure the Outbox with SQL Server

```csharp
using Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection;
using MessagingOverQueue.Persistence.DependencyInjection;

services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379"))
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
    [StreamName] NVARCHAR(256) NULL,
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
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379"))
    .AddOutboxPattern()
    .Services.AddSingleton<IMessageStoreProvider, PostgreSqlMessageStoreProvider>();
```

## Resilience Configuration

```csharp
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379")
    .WithMaxDeliveryAttempts(5))
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

## Health Checks

```csharp
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379"))
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
    Console.WriteLine($"Stream: {context.StreamName}");
    Console.WriteLine($"Consumer Group: {context.ConsumerGroup}");
    Console.WriteLine($"Delivery Count: {context.DeliveryCount}");
    Console.WriteLine($"Received At: {context.ReceivedAt}");

    var customHeader = context.Headers["x-custom-header"];
}
```

## Handler Registration Methods

### Automatic Registration (Recommended)

Scans assemblies and automatically registers all handlers:

```csharp
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379"))
    .AddTopology(topology => topology
        .WithServiceName("my-service")
        .ScanAssemblyContaining<OrderCreatedHandler>());

// Automatically registers:
// - IMessageHandler<OrderCreatedEvent> → OrderCreatedHandler (scoped)
// - HandlerInvoker<OrderCreatedEvent> in registry
// - Consumer group "my-service" on the stream
// - Message type for serialization
```

### Manual Handler Registration

For fine-grained control, register handlers explicitly:

```csharp
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379"))
    .AddHandler<OrderCreatedHandler, OrderCreatedEvent>()
    .AddHandler<PaymentProcessedHandler, PaymentProcessedEvent>();
```

### Multiple Handlers for Same Message

Register multiple handlers for a single message type:

```csharp
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379"))
    .AddHandler<EmailNotificationHandler, OrderCreatedEvent>()
    .AddHandler<AuditLoggingHandler, OrderCreatedEvent>()
    .AddHandler<AnalyticsTrackingHandler, OrderCreatedEvent>();

// When OrderCreatedEvent is received:
// 1. HandlerInvoker<OrderCreatedEvent> resolves ALL handlers from DI
// 2. Executes them sequentially in registration order
// 3. All handlers must succeed for message acknowledgment (XACK)
```

## Complete Registration Example

```csharp
services.AddRedisStreamsMessaging(options => options
    .UseConnectionString("localhost:6379")
    .WithStreamPrefix("myapp")
    .ConfigureConsumer(consumer => consumer
        .WithBatchSize(100)
        .WithMaxConcurrency(10)
        .WithClaimIdleTime(TimeSpan.FromMinutes(5)))
    .WithTimeBasedRetention(TimeSpan.FromDays(7))
    .WithDeadLetterPerConsumerGroup()
    .WithMaxDeliveryAttempts(5))

    // Handler-based auto-discovery - this does everything!
    .AddTopology(topology => topology
        .WithServiceName("my-service")
        .ScanAssemblyContaining<OrderCreatedHandler>())

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
│   ├── Donakunn.MessagingOverQueue/              # Core library (abstractions)
│   │   ├── Abstractions/
│   │   │   ├── Consuming/      # IMessageHandler, IMessageContext
│   │   │   ├── Messages/       # IMessage, IEvent, ICommand, IQuery, MessageBase
│   │   │   ├── Publishing/     # IMessagePublisher, IEventPublisher, ICommandSender
│   │   │   └── Serialization/  # IMessageSerializer, IMessageTypeResolver
│   │   ├── Configuration/
│   │   │   ├── Builders/       # MessagingOptionsBuilder, fluent builders
│   │   │   ├── Options/        # MessagingOptions, ConsumerOptions, RetryOptions
│   │   │   └── Sources/        # Configuration sources (AppSettings, Fluent)
│   │   ├── Consuming/
│   │   │   ├── Handlers/       # HandlerInvokerRegistry, HandlerInvokerFactory
│   │   │   └── Middleware/     # ConsumePipeline, DeserializationMiddleware
│   │   ├── DependencyInjection/# ServiceCollectionExtensions, IMessagingBuilder
│   │   ├── HealthChecks/       # ConnectionHealthCheck
│   │   ├── Hosting/            # ConsumerHostedService
│   │   ├── Persistence/
│   │   │   ├── DependencyInjection/ # MessageStoreProviderExtensions, IOutboxBuilder
│   │   │   ├── Entities/       # MessageStoreEntry (unified outbox/inbox)
│   │   │   ├── Providers/      # IMessageStoreProvider, MessageStoreOptions
│   │   │   │   └── SqlServer/  # SqlServerMessageStoreProvider
│   │   │   └── Repositories/   # IOutboxRepository, IInboxRepository
│   │   ├── Providers/          # IMessagingProvider, IInternalConsumer, IInternalPublisher
│   │   ├── Publishing/
│   │   │   └── Middleware/     # PublishPipeline, SerializationMiddleware
│   │   ├── Resilience/
│   │   │   └── CircuitBreaker/ # ICircuitBreaker, PollyCircuitBreaker
│   │   └── Topology/
│   │       ├── Abstractions/   # ITopologyScanner, ITopologyRegistry, ITopologyDeclarer
│   │       ├── Attributes/     # RedisConsumerGroupAttribute, etc.
│   │       ├── Builders/       # TopologyBuilder, MessageTopologyBuilder
│   │       ├── Conventions/    # DefaultTopologyNamingConvention
│   │       └── DependencyInjection/ # TopologyServiceCollectionExtensions
│   │
│   └── Donakunn.MessagingOverQueue.RedisStreams/ # Redis Streams provider
│       ├── Connection/         # RedisConnectionManager
│       ├── Consumer/           # RedisStreamsConsumer : IInternalConsumer
│       ├── Publisher/          # RedisStreamsPublisher : IInternalPublisher
│       ├── Topology/           # RedisStreamsTopologyDeclarer : ITopologyDeclarer
│       └── HealthChecks/       # RedisStreamsHealthCheck
│
└── tests/                      # Unit and integration tests
```

## Key Benefits

- **Zero-Config Handlers**: Handlers are automatically registered and connected to consumers
- **Reflection-Free Dispatch**: Handler invoker pattern eliminates per-message reflection overhead
- **O(1) Handler Lookup**: ConcurrentDictionary-based registry for instant handler resolution
- **Service Isolation**: Each service gets its own consumer group for shared events
- **Type Safety**: Strongly-typed messages and handlers with compile-time verification
- **Scoped DI**: Automatic scope management for each message handler
- **Multiple Handlers**: Native support for multiple handlers per message type
- **Concurrency Control**: Fine-grained control via batch size and max concurrency settings
- **Provider-Based Persistence**: Pluggable database providers without EF Core dependency
- **High-Performance ADO.NET**: Direct database access with optimized queries
- **Unified Message Store**: Single table for both outbox and inbox (idempotency)
- **Atomic Locking**: SQL Server's OUTPUT clause for race-condition-free message acquisition
- **Auto Schema Creation**: Database tables created automatically on startup
- **Resilience**: Built-in retry, circuit breaker, and dead letter handling
- **Automatic Message Claiming**: XAUTOCLAIM handles failed consumers automatically
- **Stream Retention**: Configure time-based or count-based message retention
- **Flexibility**: Mix auto-discovery with manual configuration
- **Production Ready**: Health checks, monitoring, and enterprise patterns
- **High Performance**: Redis-optimized connections, serialization, minimal allocations

## Requirements

- .NET 10 or later
- Redis 6.2+ (for XAUTOCLAIM support)
- SQL Server 2016+ (for SQL Server outbox provider)

## License

Apache 2.0

## Contributing

Contributions are welcome! Please visit our [GitHub repository](https://github.com/donakunn/Donakunn.MessagingOverQueue) to:
- Report issues
- Submit pull requests
- Request features
- View the source code

