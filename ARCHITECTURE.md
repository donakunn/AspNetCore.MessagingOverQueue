# AsyncronousComunication - Architecture & Technical Documentation

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Core Components](#core-components)
- [Configuration System](#configuration-system)
- [Topology Management](#topology-management)
- [Message Pipeline](#message-pipeline)
- [Resilience Patterns](#resilience-patterns)
- [Outbox Pattern](#outbox-pattern)
- [Connection Management](#connection-management)
- [Extension Points](#extension-points)

---

## Architecture Overview

The library follows a layered architecture with clean separation of concerns:

```
???????????????????????????????????????????????????????????????????
?                    Application Layer                             ?
?  (ICommandSender, IEventPublisher, IMessageHandler<T>)          ?
???????????????????????????????????????????????????????????????????
?                   Middleware Pipeline Layer                      ?
?  Publishing: Logging ? Serialization ? Topology Resolution      ?
?  Consuming: Logging ? Deserialization ? Idempotency ? Retry     ?
???????????????????????????????????????????????????????????????????
?                     Topology Layer                               ?
?  Auto-Discovery, Convention-Based Naming, Registry              ?
???????????????????????????????????????????????????????????????????
?                Infrastructure Layer                              ?
?  RabbitMqPublisher, RabbitMqConsumer, OutboxPublisher           ?
???????????????????????????????????????????????????????????????????
?                 Configuration Layer                              ?
?  Sources: Aspire ? AppSettings ? Fluent API ? Custom            ?
???????????????????????????????????????????????????????????????????
?                   Connection Pool                                ?
?  RabbitMqConnectionPool (Channel Pooling & Lifecycle)           ?
???????????????????????????????????????????????????????????????????
?                      RabbitMQ Broker                             ?
???????????????????????????????????????????????????????????????????
```

### Design Principles

1. **SOLID Principles**
   - Single Responsibility: Each class has one clear purpose
   - Open/Closed: Extensible via middleware and configuration sources
   - Liskov Substitution: Interfaces can be substituted with implementations
   - Interface Segregation: Fine-grained interfaces (ICommandSender, IEventPublisher)
   - Dependency Inversion: Depends on abstractions, not concretions

2. **Dependency Injection First**
   - All services registered via `IServiceCollection`
   - Scoped services for transactional operations (Outbox)
   - Singleton services for shared infrastructure (ConnectionPool)

3. **Asynchronous from the Ground Up**
   - All I/O operations are async
   - CancellationToken support throughout
   - Async disposal with `IAsyncDisposable`

---

## Core Components

### 1. Message Abstractions

**Location**: `Abstractions/Messages/`

#### IMessage Interface
```csharp
public interface IMessage
{
    Guid Id { get; }
    DateTime Timestamp { get; }
    string? CorrelationId { get; }
    string? CausationId { get; }
    string MessageType { get; }
}
```

#### Message Types
- **ICommand**: Point-to-point messages (one handler)
- **IEvent**: Pub/sub messages (multiple subscribers)
- **IQuery<TResult>**: Request/response pattern (future)

#### MessageBase
Abstract base class providing:
- Auto-generated `Id` (Guid)
- UTC `Timestamp`
- Correlation tracking
- Cloning with correlation IDs

### 2. Publishers

**Location**: `Publishing/`

#### RabbitMqPublisher
The core publisher implementing:
- `IMessagePublisher` - Generic publishing
- `IEventPublisher` - Event-specific
- `ICommandSender` - Command-specific

**Key Features**:
- Middleware pipeline integration
- Topology-aware routing
- Channel pooling for concurrency
- Automatic serialization

**Publishing Flow**:
```
Message ? Middleware Pipeline ? Serialize ? Resolve Topology ? 
Acquire Channel ? Publish to Exchange ? Return Channel
```

#### OutboxPublisher
Transactional publishing via outbox pattern:
- Stores messages in database
- Part of same transaction as domain changes
- Background processor publishes messages
- Ensures at-least-once delivery

### 3. Consumers

**Location**: `Consuming/`

#### RabbitMqConsumer
Manages message consumption:
- Channel lifecycle
- Message acknowledgment
- Prefetch and concurrency control
- Middleware pipeline execution

**Consumption Flow**:
```
Receive from Queue ? Middleware Pipeline ? Deserialize ? 
Handler Resolution ? Execute Handler ? ACK/NACK
```

#### ConsumerHostedService
Manages multiple consumers:
- Lifecycle management
- Parallel consumer startup
- Graceful shutdown
- DI scope per message

### 4. Message Handlers

**Location**: `Abstractions/Consuming/`

```csharp
public interface IMessageHandler<in TMessage> where TMessage : IMessage
{
    Task HandleAsync(
        TMessage message, 
        IMessageContext context, 
        CancellationToken cancellationToken);
}
```

**IMessageContext** provides:
- Message metadata (ID, timestamp, delivery count)
- Queue and routing information
- Custom headers
- Correlation tracking

---

## Configuration System

### Architecture

The configuration system uses a **Composer Pattern** with prioritized sources:

```
RabbitMqConfigurationComposer
??? AspireConfigurationSource (Priority: 25)
??? AppSettingsConfigurationSource (Priority: 50)
??? FluentConfigurationSource (Priority: 100)
??? Custom Sources (Configurable Priority)
```

### Configuration Sources

#### 1. IRabbitMqConfigurationSource
```csharp
public interface IRabbitMqConfigurationSource
{
    int Priority { get; }
    void Configure(RabbitMqOptions options);
}
```

#### 2. AspireConfigurationSource
- Reads from .NET Aspire service discovery
- Connection string format: `amqp://user:pass@host:port/vhost`
- Default priority: 25 (lowest)
- Ideal for local development

#### 3. AppSettingsConfigurationSource
- Binds from `IConfiguration`
- Supports environment variables
- Default section: "RabbitMq"
- Priority: 50
- Production-ready

#### 4. FluentConfigurationSource
- Builder pattern API
- Runtime configuration
- Highest priority: 100
- Override other sources

### Configuration Merge Strategy

```csharp
public RabbitMqOptions Build()
{
    var options = new RabbitMqOptions();
    
    // Apply sources in priority order (lowest to highest)
    foreach (var source in _sources.OrderBy(s => s.Priority))
    {
        source.Configure(options);
    }
    
    return options;
}
```

**Key Points**:
- Scalar properties (string, int) are overwritten
- Collections (Exchanges, Queues) are merged by name
- Later sources win for conflicts

### Builders

#### RabbitMqOptionsBuilder
Fluent API for connection and topology:
```csharp
.UseHost("localhost")
.UsePort(5672)
.WithCredentials("user", "pass")
.AddExchange(ex => ex.WithName("orders").AsTopic())
.AddQueue(q => q.WithName("queue").Durable())
.AddBinding(b => b.FromExchange("ex").ToQueue("q").WithRoutingKey("key"))
```

#### ExchangeBuilder
- Exchange types: Direct, Topic, Fanout, Headers
- Durability and auto-delete
- Custom arguments

#### QueueBuilder
- Durable, exclusive, auto-delete
- Dead letter configuration
- TTL, max length, overflow behavior
- Queue types: Classic, Quorum, Stream, Lazy

#### BindingBuilder
- Exchange to queue bindings
- Routing keys
- Header arguments

---

## Topology Management

### Overview

Topology management provides **automatic infrastructure provisioning** from message definitions.

### Components

#### 1. ITopologyScanner
Scans assemblies for:
- Message types (ICommand, IEvent, IQuery)
- Message handlers (IMessageHandler<T>)
- Topology attributes

```csharp
public interface ITopologyScanner
{
    IReadOnlyCollection<MessageTypeInfo> ScanForMessageTypes(params Assembly[] assemblies);
    IReadOnlyCollection<HandlerTypeInfo> ScanForHandlers(params Assembly[] assemblies);
}
```

#### 2. ITopologyNamingConvention
Generates names from message types:

```csharp
public interface ITopologyNamingConvention
{
    string GetExchangeName(Type messageType);
    string GetQueueName(Type messageType, string? serviceName);
    string GetRoutingKey(Type messageType);
    string GetDeadLetterExchangeName(Type messageType);
    string GetDeadLetterQueueName(Type messageType, string? serviceName);
}
```

**DefaultTopologyNamingConvention**:
- Events: `events.{message-name}` (topic exchange)
- Commands: `commands.{message-name}` (direct exchange)
- Queues: `{service-name}.{message-name}`
- Routing keys: `{namespace}.{message-name}`

#### 3. ITopologyRegistry
Thread-safe registry for topology metadata:
- Exchange definitions
- Queue definitions
- Binding rules
- Routing key patterns

```csharp
public interface ITopologyRegistry
{
    void RegisterExchange(ExchangeDefinition definition);
    void RegisterQueue(QueueDefinition definition);
    void RegisterBinding(BindingDefinition definition);
    ExchangeDefinition? GetExchange(string name);
    QueueDefinition? GetQueue(string name);
    IReadOnlyCollection<BindingDefinition> GetBindingsForQueue(string queueName);
}
```

#### 4. ITopologyProvider
Provides topology from message types:

```csharp
public interface ITopologyProvider
{
    TopologyMetadata GetTopologyForMessage(Type messageType);
}
```

**ConventionBasedTopologyProvider**:
1. Check attributes on message type
2. Apply naming conventions
3. Apply default policies
4. Return complete topology metadata

#### 5. ITopologyDeclarer
Declares topology on RabbitMQ:

```csharp
public interface ITopologyDeclarer
{
    Task DeclareTopologyAsync(TopologyMetadata metadata, CancellationToken cancellationToken);
    Task DeclareExchangeAsync(ExchangeDefinition definition, CancellationToken cancellationToken);
    Task DeclareQueueAsync(QueueDefinition definition, CancellationToken cancellationToken);
    Task DeclareBindingAsync(BindingDefinition definition, CancellationToken cancellationToken);
}
```

### Topology Attributes

#### MessageAttribute
```csharp
[Message(AutoDiscover = false, Version = "2.0", Group = "orders")]
public class OrderCreatedEvent : Event { }
```

#### ExchangeAttribute
```csharp
[Exchange("orders-exchange", Type = ExchangeType.Topic, Durable = true)]
public class OrderCreatedEvent : Event { }
```

#### QueueAttribute
```csharp
[Queue("orders-queue", QueueType = QueueType.Quorum, MessageTtlMs = 86400000)]
public class OrderCreatedEvent : Event { }
```

#### RoutingKeyAttribute
```csharp
[RoutingKey("orders.created.{version}")]
public class OrderCreatedEvent : Event { }
```

#### DeadLetterAttribute
```csharp
[DeadLetter("orders-dlx", QueueName = "orders-failed")]
public class OrderCreatedEvent : Event { }
```

#### RetryPolicyAttribute
```csharp
[RetryPolicy(MaxRetries = 5, InitialDelayMs = 2000)]
public class OrderCreatedEvent : Event { }
```

#### BindingAttribute
```csharp
[Binding("orders.*.created")]
[Binding("orders.*.updated")]
public class OrderEvent : Event { }
```

### Topology Auto-Discovery Flow

```
Application Start
    ?
TopologyInitializationHostedService
    ?
Scan Assemblies ? MessageTypeInfo[]
    ?
For Each Message Type:
    Check Attributes ? Apply Conventions ? Build Topology Metadata
    ?
Register in TopologyRegistry
    ?
Declare on RabbitMQ via TopologyDeclarer
    ?
Ready for Publishing/Consuming
```

### Topology Configuration Patterns

#### Pattern 1: Full Auto-Discovery
```csharp
services.AddRabbitMqMessaging(...)
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderCreatedEvent>());
```

#### Pattern 2: Convention Override
```csharp
.AddTopology(topology => topology
    .ConfigureNaming(naming =>
    {
        naming.UseLowerCase = true;
        naming.EventExchangePrefix = "events.";
    })
    .ScanAssemblyContaining<OrderCreatedEvent>());
```

#### Pattern 3: Attribute-Based
```csharp
[Exchange("custom-exchange")]
[Queue("custom-queue")]
[RoutingKey("custom.routing.key")]
public class MyEvent : Event { }
```

#### Pattern 4: Fluent Override
```csharp
.AddTopology(topology => topology
    .ScanAssemblyContaining<OrderCreatedEvent>()
    .AddTopology<OrderCreatedEvent>(msg => msg
        .WithExchangeName("override-exchange")
        .WithQueueName("override-queue")));
```

---

## Message Pipeline

### Publisher Pipeline

**Interface**: `IPublishMiddleware`

```csharp
public interface IPublishMiddleware
{
    Task InvokeAsync(
        PublishContext context, 
        Func<PublishContext, CancellationToken, Task> next, 
        CancellationToken cancellationToken);
}
```

**Built-in Middleware**:
1. **LoggingMiddleware**: Logs publish operations
2. **SerializationMiddleware**: Serializes message to bytes

**Execution Order**:
```
IMessagePublisher.PublishAsync()
    ? LoggingMiddleware
        ? SerializationMiddleware
            ? RabbitMqPublisher (actual publish)
```

### Consumer Pipeline

**Interface**: `IConsumeMiddleware`

```csharp
public interface IConsumeMiddleware
{
    Task InvokeAsync(
        ConsumeContext context, 
        Func<ConsumeContext, CancellationToken, Task> next, 
        CancellationToken cancellationToken);
}
```

**Built-in Middleware**:
1. **ConsumeLoggingMiddleware**: Logs consumption
2. **DeserializationMiddleware**: Deserializes bytes to message
3. **IdempotencyMiddleware**: Prevents duplicate processing (with Outbox)
4. **RetryMiddleware**: Handles failures with retry logic

**Execution Order**:
```
RabbitMQ Delivery
    ? ConsumeLoggingMiddleware
        ? DeserializationMiddleware
            ? IdempotencyMiddleware
                ? RetryMiddleware
                    ? Handler Invocation
```

### Custom Middleware Example

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
        try
        {
            await next(context, cancellationToken);
            _metrics.RecordSuccess(context.MessageType, sw.Elapsed);
        }
        catch (Exception ex)
        {
            _metrics.RecordFailure(context.MessageType, sw.Elapsed, ex);
            throw;
        }
    }
}

// Register
services.AddSingleton<IPublishMiddleware, MetricsMiddleware>();
```

---

## Resilience Patterns

### 1. Retry Policy

**Location**: `Resilience/RetryPolicy.cs`

**Implementation**: Polly-based retry with:
- Exponential backoff
- Jitter to prevent thundering herd
- Maximum retry attempts
- Configurable delays

```csharp
services.ConfigureRetry(retry =>
{
    retry.MaxRetryAttempts = 5;
    retry.InitialDelay = TimeSpan.FromSeconds(1);
    retry.MaxDelay = TimeSpan.FromMinutes(5);
    retry.UseExponentialBackoff = true;
    retry.AddJitter = true;
});
```

**Algorithm**:
```
Delay = Min(InitialDelay * 2^attempt, MaxDelay) + Jitter
```

### 2. Circuit Breaker

**Location**: `Resilience/CircuitBreaker.cs`

**States**: Closed ? Open ? Half-Open ? Closed

```csharp
services.AddCircuitBreaker(cb =>
{
    cb.FailureRateThreshold = 0.5; // 50% failures
    cb.SamplingDuration = TimeSpan.FromMinutes(1);
    cb.MinimumThroughput = 10; // Min calls before breaking
    cb.DurationOfBreak = TimeSpan.FromSeconds(30);
});
```

**State Transitions**:
- **Closed**: Normal operation
- **Open**: Failures exceeded threshold, fail fast
- **Half-Open**: Test if service recovered
- **Closed**: Service recovered

### 3. Dead Letter Handling

**Location**: `Resilience/DeadLetterHandler.cs`

**Features**:
- Automatic DLX configuration
- Retry exhausted messages
- Message inspection
- Poison message isolation

**Queue Arguments**:
```csharp
"x-dead-letter-exchange" = "dlx-exchange"
"x-dead-letter-routing-key" = "failed.{original-routing-key}"
```

---

## Outbox Pattern

### Architecture

```
Application Transaction
    ?
Domain Changes + OutboxMessage (same transaction)
    ?
Commit
    ?
OutboxProcessor (background)
    ?
Read Outbox ? Publish to RabbitMQ ? Mark Processed
```

### Components

#### 1. IOutboxDbContext
```csharp
public interface IOutboxDbContext
{
    DbSet<OutboxMessage> OutboxMessages { get; }
    DbSet<InboxMessage> InboxMessages { get; }
}
```

#### 2. OutboxMessage Entity
```csharp
public class OutboxMessage
{
    public Guid Id { get; set; }
    public string MessageType { get; set; }
    public byte[] Payload { get; set; }
    public string? RoutingKey { get; set; }
    public string? Exchange { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? ProcessedAt { get; set; }
    public int RetryCount { get; set; }
    public string? Error { get; set; }
}
```

#### 3. OutboxPublisher
Scoped service for transactional publishing:
```csharp
public class OutboxPublisher
{
    public async Task PublishAsync<TMessage>(TMessage message, CancellationToken ct)
        where TMessage : IMessage
    {
        var outboxMessage = CreateOutboxMessage(message);
        await _repository.AddAsync(outboxMessage, ct);
        // Message published when DbContext.SaveChanges() is called
    }
}
```

#### 4. OutboxProcessor
Background hosted service:
- Polls outbox table at intervals
- Publishes pending messages
- Updates processed status
- Handles failures with retry

```csharp
services.AddOutboxPattern<AppDbContext>(options =>
{
    options.ProcessingInterval = TimeSpan.FromSeconds(5);
    options.BatchSize = 100;
    options.RetentionPeriod = TimeSpan.FromDays(7);
});
```

### Inbox Pattern (Idempotency)

**InboxMessage** tracks processed messages:
```csharp
public class InboxMessage
{
    public Guid MessageId { get; set; }
    public DateTime ProcessedAt { get; set; }
}
```

**IdempotencyMiddleware**:
1. Check if message ID exists in inbox
2. If exists, skip processing (already handled)
3. If not, process and add to inbox
4. Ensures exactly-once semantics

---

## Connection Management

### RabbitMqConnectionPool

**Location**: `Connection/RabbitMqConnectionPool.cs`

**Features**:
- Single connection per application
- Channel pooling for concurrency
- Automatic reconnection
- Thread-safe channel management

**Architecture**:
```
Application
    ?
RabbitMqConnectionPool
    ??? IConnection (singleton)
    ??? Channel Pool
        ??? Channel 1
        ??? Channel 2
        ??? ...
        ??? Channel N
```

**Key Methods**:
```csharp
public interface IRabbitMqConnectionPool
{
    Task<IChannel> GetChannelAsync(CancellationToken ct);
    void ReturnChannel(IChannel channel);
    Task EnsureConnectedAsync(CancellationToken ct);
    ValueTask DisposeAsync();
}
```

**Channel Lifecycle**:
1. Request channel from pool
2. Use for publish/consume
3. Return to pool (don't dispose!)
4. Pool manages channel lifecycle

**Configuration**:
```csharp
.WithChannelPoolSize(20) // Number of pooled channels
```

### Connection Recovery

- Automatic reconnection on failure
- Network recovery interval
- Event handlers for connection state
- Graceful degradation

---

## Extension Points

### 1. Custom Configuration Sources

Implement `IRabbitMqConfigurationSource`:
```csharp
public class VaultConfigurationSource : IRabbitMqConfigurationSource
{
    public int Priority => 75;
    
    public void Configure(RabbitMqOptions options)
    {
        options.HostName = _vault.GetSecret("rabbitmq-host");
        options.Password = _vault.GetSecret("rabbitmq-password");
    }
}
```

### 2. Custom Serialization

Implement `IMessageSerializer`:
```csharp
public class ProtobufSerializer : IMessageSerializer
{
    public byte[] Serialize<T>(T message) where T : IMessage
    {
        // Protobuf serialization
    }
    
    public T Deserialize<T>(byte[] data) where T : IMessage
    {
        // Protobuf deserialization
    }
}

services.AddSingleton<IMessageSerializer, ProtobufSerializer>();
```

### 3. Custom Topology Naming

Implement `ITopologyNamingConvention`:
```csharp
public class CustomNamingConvention : ITopologyNamingConvention
{
    public string GetExchangeName(Type messageType)
    {
        return $"myapp.{messageType.Name.ToLower()}";
    }
    // Implement other methods...
}

services.AddSingleton<ITopologyNamingConvention, CustomNamingConvention>();
```

### 4. Custom Middleware

See [Message Pipeline](#message-pipeline) section.

### 5. Custom Message Type Resolution

Implement `IMessageTypeResolver`:
```csharp
public class CustomTypeResolver : IMessageTypeResolver
{
    public void RegisterType<T>() where T : IMessage { }
    
    public Type? ResolveType(string messageType)
    {
        // Custom type resolution logic
    }
}
```

---

## Performance Considerations

### 1. Channel Pooling
- Channels are expensive to create
- Pool size = expected concurrent operations
- Default: 10, increase for high concurrency

### 2. Prefetch Count
- Controls how many messages are buffered
- Higher = better throughput, higher memory
- Lower = better distribution, lower latency

### 3. Queue Types

| Type | Use Case | Pros | Cons |
|------|----------|------|------|
| Classic | General purpose | Flexible, well-tested | No HA guarantees |
| Quorum | High availability | Replicated, consistent | Higher resource usage |
| Stream | High throughput | Fast, scalable | Limited features |
| Lazy | Large queues | Memory efficient | Slower access |

### 4. Serialization
- JSON is default (human-readable)
- Consider MessagePack/Protobuf for performance
- Trade-off: speed vs debuggability

### 5. Batch Processing
- Outbox processor uses batching
- Consumers use prefetch for batching
- Balance batch size with latency requirements

---

## Testing Strategies

### 1. Unit Testing Handlers
```csharp
[Fact]
public async Task Handler_ProcessesMessage_Successfully()
{
    var handler = new OrderCreatedHandler(_orderService, _eventPublisher);
    var message = new OrderCreatedEvent { OrderId = Guid.NewGuid() };
    var context = new MessageContext { /* ... */ };
    
    await handler.HandleAsync(message, context, CancellationToken.None);
    
    _orderService.Verify(x => x.ProcessOrder(message.OrderId));
}
```

### 2. Integration Testing with TestContainers
```csharp
public class RabbitMqTests : IAsyncLifetime
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder().Build();
    
    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }
    
    [Fact]
    public async Task PublishConsume_EndToEnd_Works()
    {
        var services = new ServiceCollection();
        services.AddRabbitMqMessaging(options => options
            .UseHost(_container.Hostname)
            .UsePort(_container.GetMappedPublicPort(5672)));
        
        // Test publish and consume
    }
}
```

### 3. Outbox Testing
- Use in-memory database for fast tests
- Verify transactional behavior
- Test retry scenarios

---

## Best Practices

### 1. Configuration
- Use Aspire for local development
- Use appsettings.json for deployment config
- Use fluent API for runtime overrides
- Store secrets in Azure Key Vault or similar

### 2. Message Design
- Keep messages immutable (init-only properties)
- Include correlation IDs for tracing
- Version messages for schema evolution
- Use record types for conciseness

### 3. Handler Design
- Keep handlers small and focused
- Use DI for dependencies
- Handle CancellationToken properly
- Log important events

### 4. Error Handling
- Use dead letter queues for poison messages
- Implement retry with exponential backoff
- Monitor dead letter queues
- Alert on high failure rates

### 5. Topology Management
- Use auto-discovery in development
- Review generated topology before production
- Document custom configurations
- Version your topology changes

### 6. Performance
- Tune prefetch count per queue
- Monitor queue depths
- Use appropriate queue types
- Scale consumers horizontally

### 7. Monitoring
- Enable health checks
- Track message processing time
- Monitor connection state
- Alert on queue buildup

---

## Migration Guide

### From Manual Configuration to Topology Auto-Discovery

**Before**:
```csharp
services.AddRabbitMqMessaging(options => options
    .AddExchange(ex => ex.WithName("orders").AsTopic())
    .AddQueue(q => q.WithName("order-events").Durable())
    .AddBinding(b => b.FromExchange("orders").ToQueue("order-events")));
```

**After**:
```csharp
services.AddRabbitMqMessaging(options => options.UseHost("localhost"))
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .ScanAssemblyContaining<OrderCreatedEvent>());
```

### Adding Attributes to Existing Messages

```csharp
// Add attributes incrementally
[Exchange("orders-exchange")] // Step 1: Add exchange
public class OrderCreatedEvent : Event
{
    // Properties
}

[Exchange("orders-exchange")]
[Queue("orders-queue", QueueType = QueueType.Quorum)] // Step 2: Add queue
public class OrderCreatedEvent : Event { }

[Exchange("orders-exchange")]
[Queue("orders-queue", QueueType = QueueType.Quorum)]
[DeadLetter(Enabled = true)] // Step 3: Add dead letter
public class OrderCreatedEvent : Event { }
```

---

## Troubleshooting

### Common Issues

#### 1. Messages Not Being Consumed
- Check consumer is registered: `.AddConsumer("queue-name")`
- Verify handler is registered: `.AddHandler<Handler, Message>()`
- Check queue binding and routing key
- Verify message type resolution

#### 2. Topology Not Created
- Ensure `RabbitMqHostedService` is running
- Check logs for topology declaration errors
- Verify connection to RabbitMQ
- Check permissions on RabbitMQ user

#### 3. Outbox Messages Not Processing
- Verify `OutboxProcessor` is registered
- Check database connection
- Review processing interval setting
- Check for errors in outbox table

#### 4. Connection Issues
- Verify hostname and port
- Check credentials
- Review firewall rules
- Enable connection logging

### Debug Logging

Enable detailed logging:
```json
{
  "Logging": {
    "LogLevel": {
      "AsyncronousComunication": "Debug"
    }
  }
}
```

---

## Appendix: Complete Service Registration

```csharp
services.AddRabbitMqMessaging(builder.Configuration, options => options
    .WithConnectionName("MyApp")
    .WithChannelPoolSize(20))
    
    // Topology auto-discovery
    .AddTopology(topology => topology
        .WithServiceName("my-service")
        .WithDeadLetterEnabled(true)
        .ScanAssemblyContaining<MyEvent>())
    
    // Handlers
    .AddHandler<OrderCreatedHandler, OrderCreatedEvent>()
    .AddHandler<PaymentProcessedHandler, PaymentProcessedEvent>()
    
    // Consumers
    .AddConsumer("order-events", opt => opt.PrefetchCount = 10)
    .AddConsumer("payment-events", opt => opt.PrefetchCount = 20)
    
    // Outbox pattern
    .AddOutboxPattern<AppDbContext>(outbox =>
    {
        outbox.ProcessingInterval = TimeSpan.FromSeconds(5);
        outbox.BatchSize = 100;
    })
    
    // Resilience
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
    
    // Health checks
    .AddHealthChecks();
```
