# MessagingOverQueue - Architecture & Technical Documentation

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Core Components](#core-components)
- [Configuration System](#configuration-system)
- [Topology Management](#topology-management)
- [Message Pipeline](#message-pipeline)
- [Resilience Patterns](#resilience-patterns)
- [Outbox Pattern](#outbox-pattern)
- [Connection Management](#connection-management)
- [Redis Streams Provider](#redis-streams-provider)
- [Extension Points](#extension-points)

---

## Architecture Overview

The library follows a **provider-based architecture** with clear separation between:
- **Core Abstractions** (`Donakunn.MessagingOverQueue`): Provider-agnostic interfaces, middleware pipeline, and topology management
- **Concrete Providers**: RabbitMQ (built-in) and Redis Streams (separate package)

This design enables seamless switching between messaging backends without changing application code.

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Application Layer                                 │
│  (ICommandSender, IEventPublisher, IMessageHandler<T>)              │
├─────────────────────────────────────────────────────────────────────┤
│                   Middleware Pipeline Layer                          │
│  Publishing: Logging → Serialization → Topology Resolution          │
│  Consuming: Logging → Deserialization → Idempotency → Retry         │
├─────────────────────────────────────────────────────────────────────┤
│                     Topology Layer                                   │
│  Auto-Discovery, Convention-Based Naming, Registry                  │
├─────────────────────────────────────────────────────────────────────┤
│                   Provider Abstraction Layer                         │
│  IMessagingProvider, IInternalPublisher, IInternalConsumer          │
├──────────────────────────┬──────────────────────────────────────────┤
│   RabbitMQ Provider      │   Redis Streams Provider                 │
│  (Core Package)          │   (Separate Package)                     │
│                          │                                           │
│  RabbitMqPublisher       │   RedisStreamsPublisher                  │
│  RabbitMqConsumer        │   RedisStreamsConsumer                   │
│  RabbitMqConnectionPool  │   RedisConnectionPool                    │
│  RabbitMqTopologyDeclarer│   RedisStreamsTopologyDeclarer           │
├──────────────────────────┴──────────────────────────────────────────┤
│                 Configuration Layer                                  │
│  Sources: Aspire • AppSettings • Fluent API • Custom                │
├─────────────────────────────────────────────────────────────────────┤
│                    Message Broker / Storage                          │
│              RabbitMQ           │         Redis                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Provider Abstraction

The library uses a provider pattern to support multiple messaging backends. All providers implement core interfaces that abstract the underlying messaging infrastructure:

#### IMessagingProvider
Orchestrates all provider-specific components:

```csharp
public interface IMessagingProvider
{
    string ProviderName { get; }
    Task InitializeAsync(CancellationToken cancellationToken = default);
    Task<IInternalPublisher> CreatePublisherAsync(CancellationToken cancellationToken = default);
    Task<IInternalConsumer> CreateConsumerAsync(ConsumerOptions options, CancellationToken cancellationToken = default);
    Task DeclareTopologyAsync(TopologyDefinition definition, CancellationToken cancellationToken = default);
    IHealthCheck CreateHealthCheck();
}
```

#### IInternalPublisher
Provider-specific publishing logic:
- Receives serialized message from middleware pipeline
- Routes to appropriate destination (exchange/stream)
- Returns metadata (message ID, timestamp)

#### IInternalConsumer
Provider-specific consumption logic:
- Receives messages from source (queue/stream)
- Manages acknowledgments and retries
- Feeds messages into middleware pipeline

#### ITopologyDeclarer
Provider-specific topology creation:
- **RabbitMQ**: Creates exchanges, queues, and bindings
- **Redis Streams**: Creates streams and consumer groups

### Usage - Seamless Provider Switching

Application code remains unchanged when switching providers:

```csharp
// RabbitMQ
services.AddRabbitMqMessaging(config);

// Redis Streams (same abstractions, different provider)
services.AddRedisStreamsMessaging(options => 
    options.UseConnectionString("localhost:6379"));
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

**Location**: `src/Abstractions/Messages/`

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
- **IQuery<TResult>**: Request/response pattern

#### MessageBase
Abstract base class providing:
- Auto-generated `Id` (Guid)
- UTC `Timestamp`
- Correlation tracking
- Cloning with correlation IDs

### 2. Publishers

**Location**: `src/Publishing/`

#### RabbitMqPublisher
The core publisher implementing:
- `IMessagePublisher` - Generic publishing
- `IEventPublisher` - Event-specific
- `ICommandSender` - Command-specific

**Key Features**:
- Middleware pipeline integration
- Topology-aware routing via `IMessageRoutingResolver`
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

**Location**: `src/Consuming/`

#### RabbitMqConsumer
Manages message consumption:
- Dedicated channel per consumer
- Message acknowledgment
- Prefetch and concurrency control via `SemaphoreSlim`
- Middleware pipeline execution
- Processing timeout with cancellation

**Consumption Flow**:
```
Receive from Queue ? Acquire Semaphore ? Middleware Pipeline ? Deserialize ? 
Handler Resolution ? Execute Handler ? ACK/NACK ? Release Semaphore
```

#### ConsumerHostedService
Manages multiple consumers:
- Lifecycle management
- Waits for topology initialization via `TopologyReadySignal`
- Parallel consumer startup
- Graceful shutdown

### 4. Message Handlers

**Location**: `src/Abstractions/Consuming/`

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

### 5. Handler Invoker System

**Location**: `src/Consuming/Handlers/`

The handler invoker system provides reflection-free message dispatch:

#### IHandlerInvoker
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

#### HandlerInvoker<TMessage>
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

#### HandlerInvokerRegistry
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
}
```

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

**Location**: `src/Configuration/Sources/`

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

### Builders

**Location**: `src/Configuration/Builders/`

#### RabbitMqOptionsBuilder
Fluent API for connection and topology:
```csharp
.UseHost("localhost")
.UsePort(5672)
.WithCredentials("user", "pass")
.WithConnectionName("MyApp")
.WithChannelPoolSize(20)
```

---

## Topology Management

### Overview

Topology management provides **automatic infrastructure provisioning** from handler discovery.

### Components

**Location**: `src/Topology/`

#### 1. ITopologyScanner
Scans assemblies for handlers and message types:

```csharp
public interface ITopologyScanner
{
    IReadOnlyCollection<MessageTypeInfo> ScanForMessageTypes(params Assembly[] assemblies);
    IReadOnlyCollection<HandlerTypeInfo> ScanForHandlers(params Assembly[] assemblies);
    IReadOnlyCollection<HandlerTopologyInfo> ScanForHandlerTopology(params Assembly[] assemblies);
}
```

#### 2. ITopologyNamingConvention
Generates names from message types:

```csharp
public interface ITopologyNamingConvention
{
    string GetExchangeName(Type messageType);
    string GetQueueName(Type messageType);
    string GetRoutingKey(Type messageType);
    string GetDeadLetterExchangeName(string sourceQueueName);
    string GetDeadLetterQueueName(string sourceQueueName);
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

#### 4. ITopologyProvider
Provides topology from message types:

```csharp
public interface ITopologyProvider
{
    TopologyMetadata GetTopologyForMessage(Type messageType);
}
```

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

**Location**: `src/Topology/Attributes/`

#### ConsumerQueueAttribute (Handler-level)
```csharp
[ConsumerQueue(
    Name = "custom-queue",
    QueueType = QueueType.Quorum,
    PrefetchCount = 20,
    MaxConcurrency = 5)]
public class MyHandler : IMessageHandler<MyEvent> { }
```

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

### Topology Auto-Discovery Flow

```
Application Start
    ?
TopologyInitializationHostedService
    ?
TopologyScanner.ScanForHandlerTopology()
    ?
For Each Handler Found:
    ??? Register Handler in DI (scoped)
    ??? Create HandlerInvoker<TMessage>
    ??? Register in HandlerInvokerRegistry
    ??? Register Message Type for Serialization
    ??? Register ConsumerRegistration
    ?
Build TopologyDefinitions from Handler + Message Attributes
    ?
TopologyDeclarer.DeclareAsync() ? RabbitMQ
    ?
TopologyReadySignal.SetReady()
    ?
ConsumerHostedService Starts Consumers
    ?
Ready for Message Processing
```

### TopologyBuilder (Fluent API)

**Location**: `src/Topology/Builders/TopologyBuilder.cs`

```csharp
services.AddRabbitMqMessaging(config)
    .AddTopology(topology => topology
        .WithServiceName("order-service")
        .WithDeadLetterEnabled(true)
        .ConfigureNaming(naming =>
        {
            naming.UseLowerCase = true;
            naming.EventExchangePrefix = "events.";
        })
        .ConfigureProvider(provider =>
        {
            provider.DefaultDurable = true;
        })
        .ScanAssemblyContaining<OrderCreatedHandler>());
```

---

## Message Pipeline

### Publisher Pipeline

**Location**: `src/Publishing/Middleware/`

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
            ? RabbitMqPublisher.PublishToRabbitMqAsync()
```

### Consumer Pipeline

**Location**: `src/Consuming/Middleware/`

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

**Location**: `src/Resilience/`

### 1. Retry Policy

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

### 2. Circuit Breaker

**Location**: `src/Resilience/CircuitBreaker/`

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

### 3. Dead Letter Handling

Automatic DLX configuration via topology:
- Retry exhausted messages
- Poison message isolation

---

## Outbox Pattern

**Location**: `src/Persistence/`

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
    public string? ExchangeName { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? ProcessedAt { get; set; }
    public int RetryCount { get; set; }
    public string? Error { get; set; }
}
```

#### 3. OutboxPublisher
Scoped service for transactional publishing.

#### 4. OutboxProcessor
Background hosted service:
- Polls outbox table at intervals
- Publishes pending messages
- Updates processed status
- Handles failures with retry

---

## Connection Management

### RabbitMQ Connection Pool

**Location**: `src/Donakunn.MessagingOverQueue/Connection/`

**Features**:
- Single connection per application
- Channel pooling for concurrency
- Dedicated channels for consumers
- Automatic reconnection
- Thread-safe channel management

**Architecture**:
```
Application
    │
RabbitMqConnectionPool
    ├── IConnection (singleton)
    └── Channel Pool
        ├── Pooled Channels (for publishing)
        └── Dedicated Channels (for consumers)
```

### Redis Connection Pool

**Location**: `src/Donakunn.MessagingOverQueue.RedisStreams/Connection/`

**Features**:
- ConnectionMultiplexer management (singleton)
- Lazy connection establishment
- Automatic reconnection handling
- Connection event logging
- Thread-safe initialization

**Architecture**:
```
Application
    │
RedisConnectionPool
    └── ConnectionMultiplexer (singleton)
        └── IDatabase (per-operation)
```

**Key Methods**:
```csharp
public interface IRedisConnectionPool : IAsyncDisposable
{
    Task<IDatabase> GetDatabaseAsync(CancellationToken ct = default);
    Task EnsureConnectedAsync(CancellationToken ct = default);
    bool IsConnected { get; }
}
```

**Key Methods**:
```csharp
public interface IRabbitMqConnectionPool
{
    Task<IChannel> GetChannelAsync(CancellationToken ct);
    void ReturnChannel(IChannel channel);
    Task<IChannel> CreateDedicatedChannelAsync(CancellationToken ct);
    Task EnsureConnectedAsync(CancellationToken ct);
    ValueTask DisposeAsync();
}
```

---

## Redis Streams Provider

**Package**: `Donakunn.MessagingOverQueue.RedisStreams`  
**Minimum Redis Version**: 5.0+ (6.2+ recommended for XAUTOCLAIM)

The Redis Streams provider implements the same messaging abstractions using Redis Streams, providing high-throughput, durable message streaming with consumer group semantics.

### Architecture

Unlike RabbitMQ's exchange-queue model, Redis Streams uses:
- **Streams**: Append-only logs of messages (analogous to topics)
- **Consumer Groups**: Multiple consumers that load-balance messages
- **Pending Entry List (PEL)**: Tracks unacknowledged messages per consumer

### Key Components

| Component | Description | Core Responsibilities |
|-----------|-------------|----------------------|
| `RedisStreamsMessagingProvider` | Implements `IMessagingProvider` | Orchestrates all Redis Streams components |
| `RedisStreamsPublisher` | Implements `IInternalPublisher` | Publishes messages to streams with optional trimming |
| `RedisStreamsConsumer` | Implements `IInternalConsumer` | Consumes messages using XREADGROUP with XAUTOCLAIM |
| `RedisStreamsTopologyDeclarer` | Implements `ITopologyDeclarer` | Creates streams and consumer groups on startup |
| `RedisConnectionPool` | Connection management | Manages ConnectionMultiplexer lifecycle |
| `RedisStreamsHealthCheck` | Health monitoring | Verifies Redis connectivity |

### Message Flow

**Publishing Pipeline**:
```
IEventPublisher.PublishAsync()
        ↓
Middleware Pipeline (Logging, Serialization, Topology Resolution)
        ↓
RedisStreamsPublisher
        ↓
XADD {stream-key} * message-id {id} message-type {type} body {json} ...
        ↓
XTRIM {stream-key} MAXLEN ~ {maxLength} (if retention enabled)
```

**Consuming Pipeline**:
```
XREADGROUP GROUP {group} {consumer} BLOCK {ms} STREAMS {stream} >
        ↓
RedisStreamsConsumer (batched reads)
        ↓
Middleware Pipeline (Deserialization, Idempotency, Retry)
        ↓
IMessageHandler<T>.HandleAsync()
        ↓
XACK {stream} {group} {message-id} (on success)
        ↓
XADD {dlq-stream} * ... (on max retries exceeded)
```

**Idle Message Claiming** (Background Task):
```
XAUTOCLAIM {stream} {group} {consumer} {idle-time-ms} 0-0 COUNT {batch}
        ↓
Process claimed messages through pipeline
        ↓
XACK (on success) or retry claim
```

### Topology Mapping: Core Concepts → Redis Streams

| Core Abstraction | RabbitMQ | Redis Streams |
|-----------------|----------|---------------|
| **Exchange** | AMQP Exchange | Stream (append-only log) |
| **Queue** | Durable Queue | Consumer Group |
| **Binding** | Exchange-to-Queue binding | Consumer Group registration |
| **Routing Key** | Message routing | Stream key component |
| **Consumer** | Queue consumer | Consumer Group member |
| **Dead Letter** | DLX + DLQ | Separate DLQ stream |

### Stream Naming Convention

Streams are named based on the topology definition:

**Pattern**: `{prefix}:{service-name}.{message-type}`

**Examples**:
- Event: `messaging:order-service.order-created`
- Command: `messaging:payment-service.process-payment`

**Code**:
```csharp
// Topology definition (shared across providers)
var topology = new TopologyDefinition
{
    MessageType = typeof(OrderCreatedEvent),
    ExchangeName = \"order-service\",
    RoutingKey = \"order-created\"
};

// Redis Streams implementation
var streamKey = $\"{options.StreamPrefix}:{topology.ExchangeName}.{topology.RoutingKey}\";
// Result: \"messaging:order-service.order-created\"
```

### Consumer Group Pattern

**Concept**: Redis Streams consumer groups enable competing consumer semantics similar to RabbitMQ queues.

**Key Features**:
1. **Load Balancing**: Messages distributed across consumers in the same group
2. **Independent Consumption**: Each service has its own consumer group on the same stream
3. **At-Least-Once Delivery**: Messages tracked in PEL until acknowledged

**Example Scenario**:
```
Stream: messaging:order-service.order-created

Consumer Groups:
  - inventory-service   (ConsumerA, ConsumerB)  <- Load balanced
  - notification-service (ConsumerC)            <- Independent consumption
  - analytics-service    (ConsumerD, ConsumerE) <- Load balanced
```

**Implementation**:
```csharp
// Create consumer group (topology declaration)
await db.StreamCreateConsumerGroupAsync(
    streamKey,
    consumerGroup,
    StreamPosition.NewMessages,
    createStream: true);

// Consume from group
var entries = await db.StreamReadGroupAsync(
    streamKey,
    consumerGroup,
    consumerId,
    position: StreamPosition.NewMessages,
    count: batchSize);
```

### Pending Entry List (PEL) Management

The PEL tracks unacknowledged messages for each consumer.

**Automatic Claiming** (XAUTOCLAIM):
```csharp
// Periodically claim idle messages from other consumers
var claimed = await db.StreamAutoClaimAsync(
    streamKey,
    consumerGroup,
    consumerId,
    minIdleTimeMs: (long)options.ClaimIdleTime.TotalMilliseconds,
    startId: \"0-0\",
    count: batchSize);
```

**Use Cases**:
- Consumer crashes before acknowledging
- Consumer is slow/stuck processing
- Ensures no messages are lost

**Configuration**:
```csharp
services.AddRedisStreamsMessaging(options => options
    .WithClaimIdleTime(TimeSpan.FromMinutes(5))  // Claim after 5 mins idle
    .ConfigureConsumer(maxPendingMessages: 1000)); // Backpressure threshold
```

### Dead Letter Queue (DLQ) Handling

Since Redis Streams don't have native DLX, the provider implements DLQ logic:

**Flow**:
1. Track delivery attempts via `XPENDING`
2. If `deliveryCount > MaxDeliveryAttempts`:
   - Add message to DLQ stream
   - Acknowledge original message (removes from PEL)

**DLQ Naming Strategies**:

| Strategy | DLQ Stream Name | Use Case |
|----------|----------------|----------|
| **PerConsumerGroup** | `{stream}:{group}:dlq` | Isolated DLQs per service |
| **PerStream** | `{stream}:dlq` | Shared DLQ for all consumers |
| **Disabled** | N/A | Manual failure handling |

**Example**:
```csharp
// Original stream
messaging:order-service.order-created

// Per-consumer-group DLQ
messaging:order-service.order-created:inventory-service:dlq
messaging:order-service.order-created:notification-service:dlq

// Per-stream DLQ (shared)
messaging:order-service.order-created:dlq
```

**DLQ Message Format**:
```json
{
  \"message-id\": \"original-message-id\",
  \"message-type\": \"OrderCreatedEvent\",
  \"body\": \"{...}\",
  \"dlq-reason\": \"Max delivery attempts exceeded\",
  \"dlq-timestamp\": \"1736812800000\",
  \"dlq-source-stream\": \"messaging:order-service.order-created\",
  \"dlq-source-id\": \"1736812345678-0\"
}
```

### Retention Strategies

Control stream growth and memory usage:

| Strategy | Redis Command | Configuration | Use Case |
|----------|---------------|--------------|----------|
| **None** | No trimming | `RetentionStrategy.None` | Manual management |
| **CountBased** | `XTRIM MAXLEN ~ {count}` | `WithCountBasedRetention(100000)` | Fixed-size buffer |
| **TimeBased** | `XTRIM MINID ~ {timestamp}` | `WithTimeBasedRetention(TimeSpan.FromDays(7))` | Time-windowed data |

**Example**:
```csharp
services.AddRedisStreamsMessaging(options => options
    .WithCountBasedRetention(maxLength: 100_000)); // Keep last 100k messages

// Or time-based
services.AddRedisStreamsMessaging(options => options
    .WithTimeBasedRetention(TimeSpan.FromDays(7))); // Keep 7 days
```

### Configuration Options

```csharp
public class RedisStreamsOptions
{
    // Connection
    public string ConnectionString { get; set; }
    public int Database { get; set; } = 0;
    public string StreamPrefix { get; set; } = "messaging";
    
    // Consumer
    public int BatchSize { get; set; } = 100;
    public int MaxConcurrency { get; set; } = 10;
    public TimeSpan ClaimIdleTime { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan BlockingTimeout { get; set; } = TimeSpan.FromSeconds(5);
    
    // Retention
    public StreamRetentionStrategy RetentionStrategy { get; set; } = StreamRetentionStrategy.None;
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(7);
    public int MaxStreamLength { get; set; } = 100_000;
    
    // Dead Letter
    public DeadLetterStrategy DeadLetterStrategy { get; set; } = DeadLetterStrategy.Disabled;
    public int MaxDeliveryAttempts { get; set; } = 5;
}
```

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
    public byte[] Serialize<T>(T message) where T : IMessage { }
    public T Deserialize<T>(byte[] data) where T : IMessage { }
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
    public void RegisterType(Type messageType) { }
    public Type? ResolveType(string messageType) { }
}
```

---

## Performance Considerations

### 1. Handler Invoker Pattern
- O(1) lookup via `ConcurrentDictionary`
- Reflection only at startup
- Strongly-typed invocation (no `MethodInfo.Invoke`)

### 2. Channel Pooling
- Channels are expensive to create
- Pooled channels for publishing
- Dedicated channels for consumers

### 3. Prefetch Count
- Controls how many messages are buffered
- Higher = better throughput, higher memory
- Lower = better distribution, lower latency

### 4. Concurrency Control
- `SemaphoreSlim` in `RabbitMqConsumer`
- Configurable via `MaxConcurrency`
- Prevents resource exhaustion

### 5. Queue Types

| Type | Use Case | Pros | Cons |
|------|----------|------|------|
| Classic | General purpose | Flexible, well-tested | No HA guarantees |
| Quorum | High availability | Replicated, consistent | Higher resource usage |
| Stream | High throughput | Fast, scalable | Limited features |
| Lazy | Large queues | Memory efficient | Slower access |

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
- Use handler-based auto-discovery
- Review generated topology before production
- Document custom configurations
- Use `[ConsumerQueue]` for performance tuning

### 6. Performance
- Tune prefetch count per queue
- Monitor queue depths
- Use appropriate queue types
- Scale consumers horizontally

---
