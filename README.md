# MessagingOverQueue - RabbitMQ Messaging Library

A robust, high-performance asynchronous messaging library for .NET built on RabbitMQ with automatic handler-based topology discovery and SOLID design principles.

## Introduction

**MessagingOverQueue** is a production-ready RabbitMQ messaging library designed to eliminate boilerplate code and streamline message-driven architecture in .NET applications. Built with modern .NET best practices, it provides a developer-friendly abstraction over RabbitMQ while maintaining full control and flexibility.

### Why MessagingOverQueue?

Traditional RabbitMQ integration requires significant boilerplate: manual exchange and queue declarations, binding configuration, consumer setup, handler registration, and serialization plumbing. MessagingOverQueue eliminates this complexity through **intelligent handler-based auto-discovery** - simply implement `IMessageHandler<T>`, and the library automatically:

- **Discovers your handlers** at startup via assembly scanning
- **Creates RabbitMQ topology** (exchanges, queues, bindings) based on conventions or attributes
- **Registers handlers in DI** with scoped lifetime management
- **Sets up consumers** with optimized concurrency and prefetch settings
- **Dispatches messages** using reflection-free, strongly-typed handler invocation
- **Manages connections** with pooling and automatic reconnection

### Architecture Highlights

**Reflection-Free Handler Dispatch**: Unlike traditional approaches that use reflection for every message, MessagingOverQueue employs a **handler invoker registry** pattern. Generic `HandlerInvoker<TMessage>` instances are created once at startup and cached in a `ConcurrentDictionary`, providing O(1) lookup and zero reflection overhead during message processing.

**Middleware Pipeline**: Extensible middleware architecture for both publishing and consuming, enabling cross-cutting concerns like logging, serialization, validation, and enrichment.

**Connection Pooling**: Dedicated channel pool with automatic recovery, ensuring high throughput and fault tolerance.

**Topology Management**: Supports convention-based auto-discovery, attribute-based configuration, fluent API, or hybrid approaches for maximum flexibility.

**Transactional Reliability**: Built-in Outbox pattern for Entity Framework Core ensures at-least-once delivery with database transactional consistency.

### Target Scenarios

- **Microservices Communication**: Event-driven architectures, service-to-service messaging, CQRS implementations
- **Background Processing**: Asynchronous job queues, long-running tasks, scheduled workflows
- **Event Sourcing**: Publishing domain events with reliable delivery guarantees
- **Integration Patterns**: Message routing, pub/sub, request/reply, scatter-gather
- **High-Throughput Systems**: Optimized for concurrent message processing with configurable prefetch and parallelism

Whether you're building a new microservices architecture or modernizing an existing monolith, MessagingOverQueue provides the foundation for reliable, scalable messaging with minimal configuration.

---

## Features

- 🚀 **Handler-Based Auto-Discovery**: Automatically configure topology by scanning for message handlers - exchanges, queues, bindings, and consumers are all set up automatically
- ⚡ **Reflection-Free Dispatch**: Handler invoker registry eliminates reflection overhead during message processing
- 🎯 **Clean Abstractions**: Simple interfaces for publishing and consuming messages (`ICommand`, `IEvent`, `IQuery`)
- ⚙️ **Flexible Configuration**: Multiple configuration sources - Fluent API, appsettings.json, .NET Aspire, or custom sources
- 🔄 **Entity Framework Integration**: Outbox pattern for reliable message delivery with transactional consistency
- 🛡️ **Resilience**: Built-in retry policies, circuit breakers, and dead letter handling
- 🔌 **Middleware Pipeline**: Extensible pipeline for both publishing and consuming
- 💚 **Health Checks**: Built-in ASP.NET Core health check support
- 💉 **Dependency Injection**: First-class DI support with Microsoft.Extensions.DependencyInjection
- 🔗 **Connection Pooling**: Optimized channel management with automatic recovery
- 📊 **Multiple Queue Types**: Support for Classic, Quorum, Stream, and Lazy queues

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
    public List<OrderItem> Items { get; init; } = new();
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

MessagingOverQueue uses a sophisticated **handler invoker pattern** to eliminate reflection overhead during message processing. Here's how it works:

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
// 4. Caches: registry.Register(typeof(OrderCreatedEvent), invoker)
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
- Routing Key: `orders.order.created`
- Consumer: Auto-registered with default settings

### Handler with Custom Queue Configuration

Use `[ConsumerQueue]` attribute to customize the consumer's queue:

```csharp
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

#### Concurrency Control Implementation

The `RabbitMqConsumer` uses a `SemaphoreSlim` to control concurrent execution:

```csharp
// Inside RabbitMqConsumer
private readonly SemaphoreSlim _concurrencySemaphore = new(options.MaxConcurrency, options.MaxConcurrency);

private async Task OnMessageReceivedAsync(object sender, BasicDeliverEventArgs args)
{
    // Wait for available slot (blocks if MaxConcurrency reached)
    await _concurrencySemaphore.WaitAsync(_stoppingCts.Token);
    
    try
    {
        // Set timeout for handler execution
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_stoppingCts.Token);
        cts.CancelAfter(options.ProcessingTimeout);
        
        await ProcessMessageAsync(args, cts.Token);
    }
    finally
    {
        // Release slot for next message
        _concurrencySemaphore.Release();
    }
}
```

**Key Behaviors:**
- Messages are acknowledged/rejected individually (no batch ack)
- Unhandled exceptions trigger `BasicNack` with configurable requeue
- Processing timeout cancels handler and requeues message
- Graceful shutdown waits for in-flight messages to complete

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

### Option F: Legacy Message-Type Discovery

For backward compatibility, you can use the old message-type scanning:

```csharp
services.AddRabbitMqMessaging(builder.Configuration)
    .AddTopology(topology => topology
        .WithServiceName("legacy-service")
        .UseMessageTypeDiscovery()  // Use old behavior
        .ScanAssemblyContaining<OrderCreatedEvent>())
    // With legacy mode, manually register handlers and consumers
    .AddHandler<OrderCreatedHandler, OrderCreatedEvent>()
    .AddConsumer("legacy-service.order-created");
```

## Publishing Messages

```csharp
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

## Outbox Pattern

Ensure messages are published reliably within database transactions.

### 1. Configure Your DbContext

```csharp
public class AppDbContext : DbContext, IOutboxDbContext
{
    public DbSet<OutboxMessage> OutboxMessages { get; set; } = null!;
    public DbSet<InboxMessage> InboxMessages { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ConfigureOutbox();
    }
}
```

### 2. Register the Outbox Pattern

```csharp
services.AddRabbitMqMessaging(builder.Configuration)
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderHandler>())
    .AddOutboxPattern<AppDbContext>(options =>
    {
        options.ProcessingInterval = TimeSpan.FromSeconds(5);
        options.BatchSize = 100;
    });
```

### 3. Use Transactional Publishing

```csharp
public class OrderService
{
    private readonly AppDbContext _context;
    private readonly OutboxPublisher _outboxPublisher;

    public async Task CreateOrderAsync(CreateOrderCommand command)
    {
        await using var transaction = await _context.Database.BeginTransactionAsync();
        
        try
        {
            var order = new Order { /* ... */ };
            _context.Orders.Add(order);
            
            await _outboxPublisher.PublishAsync(new OrderCreatedEvent
            {
                OrderId = order.Id
            });
            
            await _context.SaveChangesAsync();
            await transaction.CommitAsync();
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }
}
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

MessagingOverQueue supports multiple approaches for registering handlers:

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

// This approach:
// ✅ Explicitly declares which handlers are active
// ✅ Allows conditional registration (e.g., feature flags)
// ✅ Supports multiple handlers per message type
// ❌ Requires manual consumer configuration
```

### Hybrid Approach

Combine auto-discovery with manual overrides:

```csharp
services.AddRabbitMqMessaging(builder.Configuration)
    .AddTopology(topology => topology
        .WithServiceName("my-service")
        .ScanAssemblyContaining<OrderCreatedHandler>())  // Auto-discover
    
    // Override specific handler configuration
    .AddHandler<CriticalPaymentHandler, PaymentEvent>()  // Additional handler
    
    // Customize consumer for specific queue
    .AddConsumer("my-service.critical-payments", opt =>
    {
        opt.PrefetchCount = 100;
        opt.MaxConcurrency = 20;
        opt.ProcessingTimeout = TimeSpan.FromMinutes(5);
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

public class EmailNotificationHandler : IMessageHandler<OrderCreatedEvent>
{
    public async Task HandleAsync(OrderCreatedEvent message, IMessageContext context, CancellationToken ct)
    {
        // Send email notification
    }
}

public class AuditLoggingHandler : IMessageHandler<OrderCreatedEvent>
{
    public async Task HandleAsync(OrderCreatedEvent message, IMessageContext context, CancellationToken ct)
    {
        // Log to audit system
    }
}
```

**Handler Execution:**
```csharp
// Inside HandlerInvoker<TMessage>
public async Task InvokeAsync(IServiceProvider serviceProvider, IMessage message, ...)
{
    // Resolves ALL registered handlers for this message type
    var handlers = serviceProvider.GetServices<IMessageHandler<TMessage>>();
    
    foreach (var handler in handlers)
    {
        await handler.HandleAsync((TMessage)message, context, cancellationToken);
    }
}
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
    
    // Optional: Manual handler registration for critical handlers
    .AddHandler<CriticalEventHandler, CriticalEvent>()
    
    // Optional: Customize consumer settings for specific queues
    .AddConsumer("my-service.critical-events", opt =>
    {
        opt.PrefetchCount = 50;
        opt.MaxConcurrency = 10;
        opt.ProcessingTimeout = TimeSpan.FromMinutes(5);
    })
    
    // Add outbox pattern
    .AddOutboxPattern<AppDbContext>(outbox =>
    {
        outbox.ProcessingInterval = TimeSpan.FromSeconds(5);
        outbox.BatchSize = 100;
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

// Result: Complete messaging infrastructure with:
// ✅ Handler auto-discovery and registration
// ✅ Reflection-free message dispatch
// ✅ Automatic topology creation
// ✅ Optimized consumer configuration
// ✅ Transactional outbox pattern
// ✅ Resilience policies
// ✅ Health monitoring
```

## Migration from Message-Type Discovery

If you're upgrading from the legacy message-type discovery:

**Before (Legacy):**
```csharp
services.AddRabbitMqMessaging(config)
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderCreatedEvent>())  // Scans messages
    .AddHandler<OrderCreatedHandler, OrderCreatedEvent>()  // Manual handler
    .AddConsumer("order-service.order-created");  // Manual consumer
```

**After (Handler-Based):**
```csharp
services.AddRabbitMqMessaging(config)
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderCreatedHandler>());  // Scans handlers - that's all!
```

## Advanced Topics

### Handler Invoker Internals

For developers interested in the low-level architecture, here's how the reflection-free handler dispatch works:

#### IHandlerInvoker Interface

```csharp
public interface IHandlerInvoker
{
    Type MessageType { get; }
    
    Task InvokeAsync(
        IServiceProvider serviceProvider,
        IMessage message,
        IMessageContext context,
        CancellationToken cancellationToken);
}
```

#### HandlerInvoker<TMessage> Implementation

```csharp
internal sealed class HandlerInvoker<TMessage> : IHandlerInvoker
    where TMessage : IMessage
{
    public Type MessageType => typeof(TMessage);

    public async Task InvokeAsync(
        IServiceProvider serviceProvider,
        IMessage message,
        IMessageContext context,
        CancellationToken cancellationToken)
    {
        // Resolve ALL handlers for this message type (supports multiple handlers)
        var handlers = serviceProvider.GetServices<IMessageHandler<TMessage>>();

        foreach (var handler in handlers)
        {
            // Strongly-typed call - no reflection, no boxing
            await handler.HandleAsync((TMessage)message, context, cancellationToken);
        }
    }
}
```

#### Handler Invoker Registry

```csharp
public sealed class HandlerInvokerRegistry : IHandlerInvokerRegistry
{
    private readonly ConcurrentDictionary<Type, IHandlerInvoker> _invokers = new();

    public IHandlerInvoker? GetInvoker(Type messageType)
    {
        return _invokers.TryGetValue(messageType, out var invoker) ? invoker : null;
    }

    public void Register(IHandlerInvoker invoker)
    {
        _invokers.TryAdd(invoker.MessageType, invoker);
    }

    public bool IsRegistered(Type messageType)
    {
        return _invokers.ContainsKey(messageType);
    }
}
```

**Why This Design?**

1. **Performance**: O(1) lookup, zero reflection after startup
2. **Type Safety**: Compile-time verification of handler signatures
3. **Multiple Handlers**: Naturally supports multiple handlers per message
4. **Testability**: Easy to mock `IHandlerInvoker` for testing
5. **Extensibility**: Can create custom invokers with different behavior (e.g., retry logic, circuit breakers)

#### Handler Invoker Factory

```csharp
public sealed class HandlerInvokerFactory : IHandlerInvokerFactory
{
    private static readonly Type HandlerInvokerOpenGeneric = typeof(HandlerInvoker<>);

    public IHandlerInvoker Create(Type messageType)
    {
        // This reflection happens ONCE at startup, not per-message
        var invokerType = HandlerInvokerOpenGeneric.MakeGenericType(messageType);
        return (IHandlerInvoker)Activator.CreateInstance(invokerType)!;
    }
}
```

### Startup Registration Flow

Understanding the complete registration flow helps with troubleshooting:

```
1. Application Startup
   ↓
2. AddRabbitMqMessaging() - Registers core services
   ↓
3. AddTopology() - Configures topology builder
   ↓
4. TopologyInitializationHostedService starts
   ↓
5. TopologyScanner.ScanForHandlerTopology()
   - Finds: IMessageHandler<T> implementations
   - Extracts: MessageType, HandlerType, ConsumerQueueAttribute
   ↓
6. For each handler found:
   a. Register in DI: services.AddScoped<IMessageHandler<T>, THandler>()
   b. Create invoker: factory.Create(typeof(T))
   c. Cache invoker: registry.Register(invoker)
   d. Register consumer: services.AddSingleton<ConsumerRegistration>()
   e. Register message type: For serialization
   ↓
7. TopologyDeclarer.DeclareAsync()
   - Creates exchanges, queues, bindings in RabbitMQ
   ↓
8. ConsumerHostedService starts
   - Creates RabbitMqConsumer for each queue
   - Consumers start receiving messages
   ↓
9. Message Processing (Runtime)
   - Consumer receives message → Deserialize
   - HandlerInvokerRegistry.GetInvoker(messageType) → O(1) lookup
   - Create DI scope → Resolve IMessageHandler<T>
   - invoker.InvokeAsync() → Strongly-typed call
   - Acknowledge/Reject message
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
✅ **Reliability**: Outbox pattern ensures messages are never lost  
✅ **Resilience**: Built-in retry, circuit breaker, and dead letter handling  
✅ **Flexibility**: Mix auto-discovery with manual configuration  
✅ **Production Ready**: Health checks, monitoring, and enterprise patterns  
✅ **High Performance**: Connection pooling, optimized serialization, minimal allocations  

## Documentation

- **[README.md](README.md)** - This file - Quick start and usage guide
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Detailed architecture and design patterns
- **Examples/** - Sample code and configuration examples

## License

APACHE 2.0

