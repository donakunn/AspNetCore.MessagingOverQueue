# MessagingOverQueue - RabbitMQ Messaging Library

A robust, high-performance asynchronous messaging library for .NET built on RabbitMQ with automatic handler-based topology discovery and SOLID design principles.

## Features

- 🚀 **Handler-Based Auto-Discovery**: Automatically configure topology by scanning for message handlers - exchanges, queues, bindings, and consumers are all set up automatically
- 🎯 **Clean Abstractions**: Simple interfaces for publishing and consuming messages (`ICommand`, `IEvent`, `IQuery`)
- ⚙️ **Flexible Configuration**: Multiple configuration sources - Fluent API, appsettings.json, .NET Aspire, or custom sources
- 🔄 **Entity Framework Integration**: Outbox pattern for reliable message delivery with transactional consistency
- 🛡️ **Resilience**: Built-in retry policies, circuit breakers, and dead letter handling
- 🔌 **Middleware Pipeline**: Extensible pipeline for both publishing and consuming
- 💚 **Health Checks**: Built-in ASP.NET Core health check support
- 💉 **Dependency Injection**: First-class DI support with Microsoft.Extensions.DependencyInjection

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
    
    // Add outbox pattern
    .AddOutboxPattern<AppDbContext>()
    
    // Configure resilience
    .ConfigureRetry(retry => retry.MaxRetryAttempts = 5)
    .AddCircuitBreaker()
    
    // Add health checks
    .AddHealthChecks();
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

## Key Benefits

✅ **Zero-Config Handlers**: Handlers are automatically registered and connected to consumers  
✅ **Service Isolation**: Each service gets its own queue for shared events  
✅ **Type Safety**: Strongly-typed messages and handlers  
✅ **Reliability**: Outbox pattern ensures messages are never lost  
✅ **Resilience**: Built-in retry, circuit breaker, and dead letter handling  
✅ **Flexibility**: Mix auto-discovery with manual configuration  
✅ **Production Ready**: Health checks, monitoring, and enterprise patterns  

## Documentation

- **[README.md](README.md)** - This file - Quick start and usage guide
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Detailed architecture and design patterns
- **Examples/** - Sample code and configuration examples

## License

APACHE 2.0

