# Redis Streams Integration Tests

This directory contains comprehensive integration tests for the **Donakunn.MessagingOverQueue.RedisStreams** provider.

## Overview

These tests validate the complete Redis Streams implementation from an external, black-box perspective. They verify that the library correctly implements messaging patterns using Redis Streams as the underlying infrastructure.

## Test Structure

### Infrastructure

**[RedisStreamsIntegrationTestBase.cs](Infrastructure/RedisStreamsIntegrationTestBase.cs)**
- Base class for all Redis Streams integration tests
- Provides containerized Redis instance using Testcontainers
- Includes Redis-specific helper methods for inspecting streams, consumer groups, and pending messages
- Manages test isolation using unique stream prefixes per test

### Test Suites

#### 1. **End-to-End Flow Tests** ([RedisStreamsEndToEndTests.cs](RedisStreamsEndToEndTests.cs))

Tests the complete message flow from publishing to consumption.

**Key Scenarios:**
- ✅ Basic publish and consume
- ✅ Message ordering preservation
- ✅ Complex payload serialization/deserialization
- ✅ High-volume message processing
- ✅ Correlation ID preservation
- ✅ Concurrent publishing thread safety
- ✅ Stream storage verification in Redis
- ✅ Multiple event types to different streams
- ✅ Concurrent message processing
- ✅ Consumer starting after publisher

**Example:**
```csharp
[Fact]
public async Task Message_Published_And_Consumed_Successfully()
{
    using var host = await BuildHost<SimpleTestEventHandler>();
    var publisher = host.Services.GetRequiredService<IEventPublisher>();

    await publisher.PublishAsync(new SimpleTestEvent { Value = "Hello Redis Streams" });
    await SimpleTestEventHandler.WaitForCountAsync(1, DefaultTimeout);

    Assert.Equal(1, SimpleTestEventHandler.HandleCount);
}
```

#### 2. **Consumer Groups Tests** ([RedisStreamsConsumerGroupsTests.cs](RedisStreamsConsumerGroupsTests.cs))

Tests Redis Streams consumer group functionality, which provides competing consumer semantics.

**Key Scenarios:**
- ✅ Consumer group creation on startup
- ✅ Load balancing across multiple consumers in same group
- ✅ Independent consumption by different groups on same stream
- ✅ Dynamic consumer joining
- ✅ Consumer group state independence
- ✅ Consumer group persistence across restarts
- ✅ No message duplication within a group
- ✅ Independent pending lists per group

**Architecture:**
```
Stream: messaging:order-service.order-created

Consumer Groups:
  - inventory-service   (Consumer-A, Consumer-B)  ← Load balanced
  - notification-service (Consumer-C)            ← Independent
  - analytics-service    (Consumer-D, Consumer-E) ← Load balanced
```

**Example:**
```csharp
[Fact]
public async Task Multiple_Consumers_In_Same_Group_Load_Balance_Messages()
{
    using var consumer1 = await BuildHost<SimpleTestEventHandler>();
    using var consumer2 = await BuildHost<SimpleTestEventHandler>();

    // Publish 20 messages
    // Both consumers will share the workload
    // Each processes ~10 messages
}
```

#### 3. **Topology Declaration Tests** ([RedisStreamsTopologyTests.cs](RedisStreamsTopologyTests.cs))

Tests automatic topology creation and management.

**Key Scenarios:**
- ✅ Stream creation on first publish
- ✅ Consumer group creation on startup
- ✅ Multiple message types create separate streams
- ✅ Stream prefix application
- ✅ Idempotent topology declaration
- ✅ Multiple services create separate consumer groups
- ✅ Stream listing in Redis
- ✅ Consumer group information accessibility
- ✅ Stream info accuracy
- ✅ Topology before consumption
- ✅ Custom queue attributes
- ✅ Multiple handlers sharing streams
- ✅ Topology persistence

**Topology Mapping:**
| Core Abstraction | RabbitMQ | Redis Streams |
|-----------------|----------|---------------|
| Exchange | AMQP Exchange | Stream |
| Queue | Durable Queue | Consumer Group |
| Binding | Exchange-to-Queue | Group Registration |

**Example:**
```csharp
[Fact]
public async Task Stream_Created_On_First_Publish()
{
    var streamKey = $"{StreamPrefix}:test-service.simple-test";
    
    await publisher.PublishAsync(new SimpleTestEvent { Value = "First" });
    
    var streamLength = await GetStreamLengthAsync(streamKey);
    Assert.True(streamLength > 0);
}
```

#### 4. **Acknowledgement Tests** ([RedisStreamsAcknowledgementTests.cs](RedisStreamsAcknowledgementTests.cs))

Tests message acknowledgement and NACK behavior.

**Key Scenarios:**
- ✅ Successful processing acknowledges message
- ✅ Failed processing leaves message in pending
- ✅ Multiple messages acknowledged independently
- ✅ Acknowledged messages not redelivered
- ✅ Slow processing eventually acknowledged
- ✅ Concurrent messages all acknowledged
- ✅ Partial batch acknowledgement
- ✅ Acknowledgement visible in stream info
- ✅ Consumer shutdown behavior
- ✅ Multiple consumers acknowledge different messages

**Flow:**
```
Message → XREADGROUP (pending) → Process → XACK (acknowledged)
                                   ↓ (failure)
                              Remains in PEL
```

**Example:**
```csharp
[Fact]
public async Task Successful_Processing_Acknowledges_Message()
{
    await publisher.PublishAsync(new SimpleTestEvent { Value = "ACK test" });
    await SimpleTestEventHandler.WaitForCountAsync(1, DefaultTimeout);

    var pendingCount = await GetPendingMessagesCountAsync(streamKey, group);
    Assert.Equal(0, pendingCount); // Message removed from pending
}
```

#### 5. **Pending Entry List (PEL) Tests** ([RedisStreamsPendingMessagesTests.cs](RedisStreamsPendingMessagesTests.cs))

Tests pending message management, idle claiming, retries, and dead letter queue behavior.

**Key Scenarios:**
- ✅ Failed messages appear in pending list
- ✅ Idle messages automatically claimed by another consumer
- ✅ Pending count decreases on acknowledgement
- ✅ Multiple failed messages tracked independently
- ✅ Pending messages have delivery count information
- ✅ Messages exceeding max attempts moved to DLQ
- ✅ DLQ messages contain original data
- ✅ Claimed messages reprocessed successfully
- ✅ PEL specific to consumer group
- ✅ High-volume pending message management

**Auto-Claim Flow:**
```
Consumer A: Processing message (slow/crashed)
                    ↓
            Message becomes idle (5 min)
                    ↓
Consumer B: XAUTOCLAIM → Re-process → XACK
```

**Dead Letter Queue:**
```
Message → Retry 1 → Retry 2 → Retry 3 (max) → XADD to DLQ → XACK original
```

**Example:**
```csharp
[Fact]
public async Task Idle_Messages_Automatically_Claimed_By_Another_Consumer()
{
    // Consumer1: Process message slowly, then stop
    using (var consumer1 = ...) { /* message left pending */ }
    
    // Consumer2: Starts and claims idle message
    using var consumer2 = await BuildHost<ClaimableEventHandler>();
    
    // Message automatically claimed and processed
}
```

## Test Helpers

### Redis-Specific Helpers

The base class provides extensive helpers for inspecting Redis state:

```csharp
// Stream inspection
await GetStreamMessagesAsync(streamKey);
await GetStreamLengthAsync(streamKey);

// Consumer group inspection
await ConsumerGroupExistsAsync(streamKey, groupName);
await GetConsumersAsync(streamKey, groupName);

// Pending message inspection
await GetPendingMessagesCountAsync(streamKey, groupName);

// Direct Redis operations
await AddStreamMessageAsync(streamKey, fields);
await DeleteStreamAsync(streamKey);
await GetKeysAsync(pattern);
```

## Test Execution Context

Tests use the **TestExecutionContext** pattern for isolation:

```csharp
protected RedisStreamsIntegrationTestBase()
{
    _testContext = new TestExecutionContext();
    TestExecutionContextAccessor.Current = _testContext;
}
```

This ensures:
- No state sharing between parallel tests
- Thread-safe handler counters and collectors
- Proper cleanup after each test

## Test Data & Handlers

Tests reuse existing test doubles from `TestDoubles/TestMessagesAndHandlers.cs`:

- `SimpleTestEvent` / `SimpleTestEventHandler`
- `ComplexTestEvent` / `ComplexTestEventHandler`
- `SlowProcessingEvent` / `SlowProcessingEventHandler`
- `FailingEvent` / `FailingEventHandler`
- And more...

Additional handlers are defined within test files for specific scenarios (e.g., `ClaimableEventHandler`, `RetryableEventHandler`).

## Running the Tests

### Prerequisites

- Docker (for Testcontainers)
- .NET 9.0 SDK

### Execute All Redis Streams Tests

```bash
dotnet test --filter "FullyQualifiedName~RedisStreams"
```

### Execute Specific Test Suite

```bash
dotnet test --filter "FullyQualifiedName~RedisStreamsEndToEndTests"
dotnet test --filter "FullyQualifiedName~RedisStreamsConsumerGroupsTests"
dotnet test --filter "FullyQualifiedName~RedisStreamsTopologyTests"
dotnet test --filter "FullyQualifiedName~RedisStreamsAcknowledgementTests"
dotnet test --filter "FullyQualifiedName~RedisStreamsPendingMessagesTests"
```

### Execute Single Test

```bash
dotnet test --filter "FullyQualifiedName~Message_Published_And_Consumed_Successfully"
```

## Test Configuration

Tests use containerized Redis with sensible defaults:

```csharp
services.AddRedisStreamsMessaging(options =>
{
    options.UseConnectionString(RedisConnectionString);
    options.WithStreamPrefix(StreamPrefix); // Unique per test
    options.ConfigureConsumer(batchSize: 10, maxConcurrency: 10);
    options.WithClaimIdleTime(TimeSpan.FromSeconds(5)); // Fast for testing
    options.WithCountBasedRetention(10000);
});
```

## Architecture Validation

These tests validate the provider-agnostic architecture:

✅ **Core abstractions work unchanged** (`IEventPublisher`, `IMessageHandler<T>`)  
✅ **Middleware pipeline functions correctly** (Serialization, Logging, Idempotency)  
✅ **Topology auto-discovery works** (Handler scanning, convention-based naming)  
✅ **Provider-specific behavior correct** (Streams, Consumer Groups, PEL, DLQ)

## Coverage Summary

| Category | Tests | Coverage |
|----------|-------|----------|
| **End-to-End Flow** | 12 | Complete message lifecycle |
| **Consumer Groups** | 9 | Load balancing, isolation |
| **Topology** | 13 | Stream/group creation |
| **Acknowledgement** | 10 | ACK/NACK behavior |
| **Pending Messages** | 10 | PEL, claiming, DLQ |
| **Total** | **54** | Production-ready |

## Design Principles

These tests follow SOLID principles:

1. **Single Responsibility**: Each test validates one specific scenario
2. **Open/Closed**: Base class extensible, test cases focused
3. **Liskov Substitution**: Provider abstraction validated
4. **Interface Segregation**: Test helpers granular
5. **Dependency Inversion**: Tests depend on abstractions, not implementations

## Quality Standards

✅ **Clean Code**: Descriptive test names, clear arrange-act-assert structure  
✅ **SOLID Compliant**: Proper abstraction and separation of concerns  
✅ **Highly Structured**: Consistent patterns across all test suites  
✅ **Maintainable**: Reusable base class and test helpers  
✅ **Production-Ready**: Comprehensive scenario coverage

## Future Enhancements

Potential additions:

- Performance/benchmark tests
- Chaos engineering tests (network failures, Redis crashes)
- Multi-region/distributed tests
- Stream compaction/retention tests
- Custom serialization tests
- Health check integration tests

---

**Last Updated**: January 13, 2026  
**Test Framework**: xUnit + Testcontainers  
**Redis Version**: 7.x (via Docker)
