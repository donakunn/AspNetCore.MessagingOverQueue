# AsyncronousComunication - RabbitMQ Messaging Library

A robust, high-performance asynchronous messaging library for .NET built on RabbitMQ with automatic topology management and SOLID design principles.

## Features

- 🚀 **Auto-Discovery Topology**: Automatically configure exchanges, queues, and bindings from message attributes or conventions
- 🎯 **Clean Abstractions**: Simple interfaces for publishing and consuming messages (`ICommand`, `IEvent`, `IQuery`)
- ⚙️ **Flexible Configuration**: Multiple configuration sources - Fluent API, appsettings.json, .NET Aspire, or custom sources
- 🔄 **Entity Framework Integration**: Outbox pattern for reliable message delivery with transactional consistency
- 🛡️ **Resilience**: Built-in retry policies, circuit breakers, and dead letter handling
- 🔌 **Middleware Pipeline**: Extensible pipeline for both publishing and consuming
- 💚 **Health Checks**: Built-in ASP.NET Core health check support
- 💉 **Dependency Injection**: First-class DI support with Microsoft.Extensions.DependencyInjection

## Installation

```bash
dotnet add package AsyncronousComunication
```

## Quick Start

### 1. Define Your Messages

```csharp
using AsyncronousComunication.Abstractions.Messages;

// Command - handled by exactly one consumer
public class CreateOrderCommand : Command
{
    public string CustomerId { get; init; } = string.Empty;
    public List<OrderItem> Items { get; init; } = new();
}

// Event - can be consumed by multiple subscribers
public class OrderCreatedEvent : Event
{
    public Guid OrderId { get; init; }
    public string CustomerId { get; init; } = string.Empty;
    public decimal TotalAmount { get; init; }
}
```

### 2. Configure Services (Choose Your Approach)

#### ⭐ Option A: Auto-Discovery with Conventions (Recommended)

```csharp
services.AddRabbitMqMessaging(builder.Configuration)
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderCreatedEvent>());
```

This automatically:
- ✅ Scans for message types in your assembly
- ✅ Creates exchanges based on message type (events → topic, commands → direct)
- ✅ Creates queues with service-specific names
- ✅ Sets up bindings with smart routing keys
- ✅ Configures dead letter queues (optional)

#### Option B: Attribute-Based Topology

```csharp
using AsyncronousComunication.Topology.Attributes;

[Exchange("orders-exchange", Type = ExchangeType.Topic)]
[Queue("order-events-queue", QueueType = QueueType.Quorum)]
[RoutingKey("orders.created")]
[DeadLetter("orders-dlx")]
public class OrderCreatedEvent : Event
{
    public Guid OrderId { get; init; }
    // ...
}

// Just scan the assembly - topology is in the attributes
services.AddRabbitMqMessaging(builder.Configuration)
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderCreatedEvent>());
```

#### Option C: Manual Configuration

```csharp
services.AddRabbitMqMessaging(options => options
    .UseHost("localhost")
    .UsePort(5672)
    .WithCredentials("guest", "guest")
    .AddExchange(ex => ex
        .WithName("orders-exchange")
        .AsTopic()
        .Durable())
    .AddQueue(q => q
        .WithName("orders-queue")
        .Durable())
    .AddBinding(b => b
        .FromExchange("orders-exchange")
        .ToQueue("orders-queue")
        .WithRoutingKey("orders.#")));
```

#### Option D: Configuration from appsettings.json

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

#### Option E: .NET Aspire Integration

```csharp
// In Program.cs
services.AddRabbitMqMessagingFromAspire(builder.Configuration);
```

### 3. Register Handlers

```csharp
public class OrderCreatedHandler : IMessageHandler<OrderCreatedEvent>
{
    private readonly ILogger<OrderCreatedHandler> _logger;
    private readonly IOrderService _orderService;

    public OrderCreatedHandler(ILogger<OrderCreatedHandler> logger, IOrderService orderService)
    {
        _logger = logger;
        _orderService = orderService;
    }

    public async Task HandleAsync(
        OrderCreatedEvent message, 
        IMessageContext context, 
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Processing order {OrderId}", message.OrderId);
        await _orderService.ProcessOrderAsync(message.OrderId, cancellationToken);
    }
}

// Register handler and consumer
services.AddRabbitMqMessaging(builder.Configuration)
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderCreatedEvent>())
    .AddHandler<OrderCreatedHandler, OrderCreatedEvent>()
    .AddConsumer("order-service.order-created", options => {
        options.PrefetchCount = 10;
        options.MaxConcurrency = 5;
    });
```

### 4. Publish Messages

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
        // Send a command
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
        // Publish an event
        await _eventPublisher.PublishAsync(new OrderShippedEvent
        {
            OrderId = id,
            ShippedAt = DateTime.UtcNow
        });
        
        return Ok();
    }
}
```

## Topology Auto-Discovery

The library can automatically create RabbitMQ infrastructure from your message definitions.

### Convention-Based (Zero Configuration)

```csharp
public class OrderCreatedEvent : Event
{
    public Guid OrderId { get; init; }
}

services.AddRabbitMqMessaging(config)
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderCreatedEvent>());
```

**Generated Topology**:
- Exchange: `events.order-created` (topic, durable)
- Queue: `order-service.order-created` (durable)
- Routing Key: `orders.order.created`
- Dead Letter: `dlx.order-created` (if enabled)

### Attribute-Based (Fine-Grained Control)

```csharp
[Exchange("payments", Type = ExchangeType.Topic)]
[Queue("payment-processing", QueueType = QueueType.Quorum)]
[RoutingKey("payments.processed")]
[DeadLetter("payments-dlx", QueueName = "payments-failed")]
[RetryPolicy(MaxRetries = 5, InitialDelayMs = 2000)]
public class PaymentProcessedEvent : Event
{
    public Guid PaymentId { get; init; }
    public decimal Amount { get; init; }
}
```

### Customize Naming Conventions

```csharp
.AddTopology(topology => topology
    .WithServiceName("my-service")
    .ConfigureNaming(naming =>
    {
        naming.UseLowerCase = true;
        naming.EventExchangePrefix = "events.";
        naming.CommandExchangePrefix = "commands.";
        naming.DeadLetterExchangePrefix = "dlx.";
    })
    .ScanAssemblyContaining<OrderCreatedEvent>());
```

## Outbox Pattern for Reliable Messaging

Ensure messages are published reliably within database transactions.

### 1. Configure Your DbContext

```csharp
using AsyncronousComunication.Persistence;
using Microsoft.EntityFrameworkCore;

public class AppDbContext : DbContext, IOutboxDbContext
{
    public DbSet<OutboxMessage> OutboxMessages { get; set; } = null!;
    public DbSet<InboxMessage> InboxMessages { get; set; } = null!;
    public DbSet<Order> Orders { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ConfigureOutbox();
    }
}
```

### 2. Register the Outbox Pattern

```csharp
services.AddRabbitMqMessaging(builder.Configuration)
    .AddTopology(...)
    .AddOutboxPattern<AppDbContext>(options =>
    {
        options.ProcessingInterval = TimeSpan.FromSeconds(5);
        options.BatchSize = 100;
        options.RetentionPeriod = TimeSpan.FromDays(7);
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
            // Create order
            var order = new Order { /* ... */ };
            _context.Orders.Add(order);
            
            // Add message to outbox (in same transaction)
            await _outboxPublisher.PublishAsync(new OrderCreatedEvent
            {
                OrderId = order.Id,
                CustomerId = command.CustomerId
            });
            
            await _context.SaveChangesAsync(); // Commits both order and outbox message
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

## Advanced Configuration

### Resilience Patterns

```csharp
services.AddRabbitMqMessaging(config)
    .AddTopology(...)
    .ConfigureRetry(retry =>
    {
        retry.MaxRetryAttempts = 5;
        retry.InitialDelay = TimeSpan.FromSeconds(1);
        retry.MaxDelay = TimeSpan.FromMinutes(5);
        retry.UseExponentialBackoff = true;
        retry.AddJitter = true;
    })
    .AddCircuitBreaker(cb =>
    {
        cb.FailureRateThreshold = 0.5;
        cb.SamplingDuration = TimeSpan.FromMinutes(1);
        cb.DurationOfBreak = TimeSpan.FromSeconds(30);
    });
```

### Queue Types for Different Scenarios

```csharp
// High Availability - Quorum Queue
[Queue("critical-queue", QueueType = QueueType.Quorum)]
public class CriticalEvent : Event { }

// High Throughput - Stream Queue
[Queue("telemetry-stream", QueueType = QueueType.Stream)]
[DeadLetter(Enabled = false)] // Streams don't support DLX
public class TelemetryEvent : Event { }

// Large Queues - Lazy Queue
[Queue("bulk-queue", QueueType = QueueType.Lazy, MaxLength = 1000000)]
public class BulkEvent : Event { }
```

### Health Checks

```csharp
services.AddRabbitMqMessaging(config)
    .AddTopology(...)
    .AddHealthChecks();

// In Program.cs
app.MapHealthChecks("/health");
```

### Custom Middleware

```csharp
public class MetricsMiddleware : IPublishMiddleware
{
    private readonly IMetrics _metrics;

    public async Task InvokeAsync(
        PublishContext context, 
        Func<PublishContext, CancellationToken, Task> next, 
        CancellationToken cancellationToken)
    {
        var sw = Stopwatch.StartNew();
        await next(context, cancellationToken);
        _metrics.RecordPublishDuration(context.MessageType, sw.ElapsedMilliseconds);
    }
}

services.AddSingleton<IPublishMiddleware, MetricsMiddleware>();
```

## Configuration Best Practices

### Development
Use .NET Aspire for local RabbitMQ orchestration:
```csharp
services.AddRabbitMqMessagingFromAspire(builder.Configuration);
```

### Testing
Use appsettings.json for repeatable configuration:
```csharp
services.AddRabbitMqMessaging(builder.Configuration);
```

### Production
Combine appsettings with secure credential management:
```csharp
services.AddRabbitMqMessaging(
    builder.Configuration,
    options => options
        // Override credentials from Key Vault
        .WithCredentials(
            builder.Configuration["KeyVault:RabbitMQ:Username"],
            builder.Configuration["KeyVault:RabbitMQ:Password"]));
```

## Message Context

Access message metadata in your handlers:

```csharp
public async Task HandleAsync(MyCommand command, IMessageContext context, CancellationToken ct)
{
    Console.WriteLine($"Message ID: {context.MessageId}");
    Console.WriteLine($"Correlation ID: {context.CorrelationId}");
    Console.WriteLine($"Queue: {context.QueueName}");
    Console.WriteLine($"Delivery Count: {context.DeliveryCount}");
    Console.WriteLine($"Received At: {context.ReceivedAt}");
    
    // Access custom headers
    var customHeader = context.Headers["x-custom-header"];
}
```

## Common Patterns

### Multi-Service Event Subscription

Different services can subscribe to the same events with their own queues:

```csharp
// Order Service
services.AddRabbitMqMessaging(config)
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<PaymentProcessedEvent>());
// Queue: order-service.payment-processed

// Notification Service
services.AddRabbitMqMessaging(config)
    .AddTopology(topology => topology
        .WithServiceName("notification-service")
        .ScanAssemblyContaining<PaymentProcessedEvent>());
// Queue: notification-service.payment-processed

// Both queues bound to: events.payment-processed
```

### Complete Registration Example

```csharp
services.AddRabbitMqMessaging(builder.Configuration, options => options
    .WithConnectionName("MyApp")
    .WithChannelPoolSize(20))
    
    // Auto-discover topology from messages
    .AddTopology(topology => topology
        .WithServiceName("my-service")
        .WithDeadLetterEnabled(true)
        .ScanAssemblyContaining<MyEvent>())
    
    // Register handlers
    .AddHandler<OrderCreatedHandler, OrderCreatedEvent>()
    .AddHandler<PaymentProcessedHandler, PaymentProcessedEvent>()
    
    // Configure consumers
    .AddConsumer("my-service.order-created", opt => opt.PrefetchCount = 10)
    .AddConsumer("my-service.payment-processed", opt => opt.PrefetchCount = 20)
    
    // Add outbox pattern
    .AddOutboxPattern<AppDbContext>()
    
    // Configure resilience
    .ConfigureRetry(retry => retry.MaxRetryAttempts = 5)
    .AddCircuitBreaker()
    
    // Add health checks
    .AddHealthChecks();
```

## Documentation

- **[README.md](README.md)** - This file - Quick start and usage guide
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Detailed architecture, design patterns, and technical documentation
- **Examples/** - Sample code and configuration examples

## Key Benefits

✅ **Rapid Development**: Auto-discovery eliminates boilerplate topology configuration  
✅ **Type Safety**: Strongly-typed messages and handlers  
✅ **Reliability**: Outbox pattern ensures messages are never lost  
✅ **Resilience**: Built-in retry, circuit breaker, and dead letter handling  
✅ **Flexibility**: Multiple configuration approaches for different scenarios  
✅ **Testability**: Clean abstractions and DI-friendly design  
✅ **Production Ready**: Health checks, monitoring, and enterprise patterns  

## Support & Contributing

- **Issues**: Report bugs or request features on GitHub
- **Documentation**: See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed technical information
- **Examples**: Check the `Examples/` folder for common patterns

## License

APACHE 2.0

