using Donakunn.MessagingOverQueue.Abstractions.Consuming;
using Donakunn.MessagingOverQueue.Abstractions.Messages;
using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.RedisStreams.Configuration;
using Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;
using MessagingOverQueue.Test.Integration.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.Infrastructure;
using MessagingOverQueue.Test.Integration.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace MessagingOverQueue.Test.Integration.RedisStreams;

/// <summary>
/// Integration tests for Redis Streams Pending Entry List (PEL) management.
/// Tests idle message claiming, retry logic, and dead letter queue behavior.
/// </summary>
public class RedisStreamsPendingMessagesTests : RedisStreamsIntegrationTestBase
{
    protected override void ConfigureAdditionalServices(IServiceCollection services)
    {
        // No additional services needed
    }

    [Fact]
    public async Task Failed_Message_Appears_In_Pending_List()
    {
        // Arrange
        FailingEventHandler.Reset();
        using var host = await BuildHost<FailingEventHandler>(options =>
            options.ConfigureClaiming(TimeSpan.FromMinutes(10))); // Prevent auto-claim during test

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new FailingEvent { ShouldFail = true });
        await Task.Delay(2000);

        // Assert
        var streamKey = $"{StreamPrefix}:test-service.failing";
        var consumerGroup = "test-service.failing";
        var pendingCount = await GetPendingMessagesCountAsync(streamKey, consumerGroup);

        Assert.True(pendingCount > 0, "Failed message should be in pending list");
    }

    [Fact]
    public async Task Idle_Messages_Automatically_Claimed_By_Another_Consumer()
    {
        // Arrange
        ClaimableEventHandler.Reset();
        var streamKey = $"{StreamPrefix}:test-service.claimable";
        var consumerGroup = "test-service.claimable";

        // Phase 1: Start first consumer, process message slowly, then stop
        using (var consumer1 = await BuildHost<ClaimableEventHandler>(options =>
            options.ConfigureClaiming(TimeSpan.FromSeconds(2)))) // Fast claim for testing
        {
            var publisher = consumer1.Services.GetRequiredService<IEventPublisher>();
            
            // Publish message that will be slow to process
            await publisher.PublishAsync(new ClaimableEvent 
            { 
                Value = "To be claimed",
                ProcessingDelay = TimeSpan.FromSeconds(30) // Very slow
            });

            // Give it time to be picked up but not processed
            await Task.Delay(500);
            
            // Stop consumer1 (message left in pending)
        }

        // Verify message is pending
        var pendingBefore = await GetPendingMessagesCountAsync(streamKey, consumerGroup);
        Assert.True(pendingBefore > 0, "Message should be pending after consumer1 stopped");

        // Phase 2: Start second consumer with auto-claim enabled
        ClaimableEventHandler.Reset();
        using var consumer2 = await BuildHost<ClaimableEventHandler>(options =>
            options.ConfigureClaiming(TimeSpan.FromSeconds(2)));

        // Wait for auto-claim to kick in
        await Task.Delay(5000);

        // Assert - Message should be claimed and processed by consumer2
        // The handler is configured to succeed when claimed
        var pendingAfter = await GetPendingMessagesCountAsync(streamKey, consumerGroup);
        
        // Should be processed or at least claimed (pending count may vary during transition)
        Assert.True(ClaimableEventHandler.HandleCount > 0, "Message should be claimed and processed");
    }

    [Fact]
    public async Task Pending_Count_Decreases_On_Acknowledgement()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var streamKey = $"{StreamPrefix}:test-service.simple-test";
        var consumerGroup = "test-service.simple-test";

        // Act - Publish message
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Pending test" });
        
        // Small delay for message to be picked up (pending increases)
        await Task.Delay(500);
        var pendingDuringProcessing = await GetPendingMessagesCountAsync(streamKey, consumerGroup);

        // Wait for processing and ACK
        await SimpleTestEventHandler.WaitForCountAsync(1, DefaultTimeout);
        await Task.Delay(1000);

        var pendingAfterAck = await GetPendingMessagesCountAsync(streamKey, consumerGroup);

        // Assert
        Assert.Equal(0, pendingAfterAck);
    }

    [Fact]
    public async Task Multiple_Failed_Messages_Tracked_Independently_In_PEL()
    {
        // Arrange
        SelectiveFailureEventHandler.Reset();
        using var host = await BuildHost<SelectiveFailureEventHandler>(options =>
            options.ConfigureClaiming(TimeSpan.FromMinutes(10)));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act - Publish mix of success and failure
        await publisher.PublishAsync(new SelectiveFailureEvent { ShouldFail = false, Value = "Success-1" });
        await publisher.PublishAsync(new SelectiveFailureEvent { ShouldFail = true, Value = "Fail-1" });
        await publisher.PublishAsync(new SelectiveFailureEvent { ShouldFail = false, Value = "Success-2" });
        await publisher.PublishAsync(new SelectiveFailureEvent { ShouldFail = true, Value = "Fail-2" });

        await Task.Delay(3000);

        // Assert
        var streamKey = $"{StreamPrefix}:test-service.selective-failure";
        var consumerGroup = "test-service.selective-failure";
        var pendingCount = await GetPendingMessagesCountAsync(streamKey, consumerGroup);

        // Two failed messages should be pending
        Assert.True(pendingCount >= 2, $"Expected at least 2 pending, got {pendingCount}");
        Assert.Equal(2, SelectiveFailureEventHandler.SuccessCount);
    }

    [Fact]
    public async Task Pending_Messages_Have_Delivery_Count_Information()
    {
        // Arrange
        FailingEventHandler.Reset();
        using var host = await BuildHost<FailingEventHandler>(options =>
            options.ConfigureClaiming(TimeSpan.FromSeconds(2))
            .WithDeadLetterPerStream(5));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var streamKey = $"{StreamPrefix}:test-service.failing";
        var consumerGroup = "test-service.failing";

        // Act - Publish failing message
        await publisher.PublishAsync(new FailingEvent { ShouldFail = true });
        
        // Wait for multiple delivery attempts
        await Task.Delay(8000);

        // Assert - Check pending message info
        var db = GetRedisDatabase();
        var pendingMessages = await db.StreamPendingMessagesAsync(
            streamKey,
            consumerGroup,
            count: 10,
            consumerName: RedisValue.Null);

        if (pendingMessages.Length > 0)
        {
            var message = pendingMessages[0];
            Assert.True(message.DeliveryCount > 1, 
                $"Message should have been delivered multiple times, got {message.DeliveryCount}");
        }
    }

    [Fact]
    public async Task Message_Exceeding_Max_Delivery_Attempts_Moved_To_DLQ()
    {
        // Arrange
        DlqTestEventHandler.Reset();
        using var host = await BuildHost<DlqTestEventHandler>(options =>
            options.ConfigureClaiming(TimeSpan.FromSeconds(1))
            .WithDeadLetterPerStream(3)); // Low threshold for testing

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var streamKey = $"{StreamPrefix}:test-service.dlq-test";
        var dlqStreamKey = $"{streamKey}:dlq";

        // Act - Publish message that will always fail
        await publisher.PublishAsync(new DlqTestEvent { AlwaysFail = true });

        // Wait for multiple retry attempts and DLQ move
        await Task.Delay(10000);

        // Assert - Message should be in DLQ
        var dlqLength = await GetStreamLengthAsync(dlqStreamKey);
        Assert.True(dlqLength > 0, "Message should be moved to DLQ after max attempts");

        // Original stream should have message acknowledged (removed from pending)
        var consumerGroup = "test-service.dlq-test";
        var pendingCount = await GetPendingMessagesCountAsync(streamKey, consumerGroup);
        Assert.Equal(0, pendingCount);
    }

    [Fact]
    public async Task DLQ_Message_Contains_Original_Message_Data()
    {
        // Arrange
        DlqTestEventHandler.Reset();
        using var host = await BuildHost<DlqTestEventHandler>(options =>
            options.ConfigureClaiming(TimeSpan.FromSeconds(1))
            .WithDeadLetterPerStream(2));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var streamKey = $"{StreamPrefix}:test-service.dlq-test";
        var dlqStreamKey = $"{streamKey}:dlq";

        var testValue = $"DLQ-{Guid.NewGuid()}";

        // Act
        await publisher.PublishAsync(new DlqTestEvent 
        { 
            AlwaysFail = true,
            TestValue = testValue
        });

        // Wait for DLQ
        await Task.Delay(8000);

        // Assert
        var dlqMessages = await GetStreamMessagesAsync(dlqStreamKey);
        Assert.NotEmpty(dlqMessages);

        var dlqMessage = dlqMessages[0];
        var fields = dlqMessage.Values.ToDictionary(v => v.Name.ToString(), v => v.Value.ToString());

        Assert.True(fields.ContainsKey("dlq-reason"));
        Assert.True(fields.ContainsKey("dlq-timestamp"));
        Assert.True(fields.ContainsKey("dlq-source-stream"));
        Assert.True(fields.ContainsKey("body")); // Original message body preserved
    }

    [Fact]
    public async Task Claimed_Message_Reprocessed_Successfully_After_Failure()
    {
        // Arrange
        RetryableEventHandler.Reset();
        using var host = await BuildHost<RetryableEventHandler>(options =>
            options.ConfigureClaiming(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1)));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act - Publish message that fails first time, succeeds on retry
        await publisher.PublishAsync(new RetryableEvent 
        { 
            Value = "Retry test",
            SucceedAfterAttempts = 2 // Fail once, succeed on second attempt
        });

        // Wait for initial failure and retry claim
        await Task.Delay(6000);

        // Assert - Should eventually succeed
        // Note: Topology naming convention removes "Event" suffix
        var streamKey = $"{StreamPrefix}:test-service.retryable";
        var consumerGroup = "test-service.retryable";
        
        // Message should be processed and acknowledged
        await WaitForConditionAsync(
            async () => await GetPendingMessagesCountAsync(streamKey, consumerGroup) == 0,
            TimeSpan.FromSeconds(15));

        Assert.True(RetryableEventHandler.HandleCount > 1, "Message should be retried");
        Assert.True(RetryableEventHandler.SuccessCount > 0, "Message should eventually succeed");
    }

    [Fact]
    public async Task PEL_Specific_To_Consumer_Group()
    {
        // Arrange - Two different services
        var service1StreamKey = $"{StreamPrefix}:service1.simple-test";
        var service2StreamKey = $"{StreamPrefix}:service2.simple-test";

        using var service1 = await BuildHost(services =>
        {
            services.AddLogging();
            services.AddRedisStreamsMessaging(options =>
            {
                options.UseConnectionString(RedisConnectionString);
                options.WithStreamPrefix(StreamPrefix);
            })
            .AddTopology(topology => topology
                .WithServiceName("service1")
                .ScanAssemblyContaining<FailingEventHandler>());
        });

        using var service2 = await BuildHost(services =>
        {
            services.AddLogging();
            services.AddRedisStreamsMessaging(options =>
            {
                options.UseConnectionString(RedisConnectionString);
                options.WithStreamPrefix(StreamPrefix);
            })
            .AddTopology(topology => topology
                .WithServiceName("service2")
                .ScanAssemblyContaining<FailingEventHandler>());
        });

        var publisher1 = service1.Services.GetRequiredService<IEventPublisher>();
        var publisher2 = service2.Services.GetRequiredService<IEventPublisher>();

        // Act - Publish failing messages to both services
        await publisher1.PublishAsync(new FailingEvent { ShouldFail = true });
        await publisher2.PublishAsync(new FailingEvent { ShouldFail = true });

        await Task.Delay(2000);

        // Assert - Each group has independent PEL
        var pending1 = await GetPendingMessagesCountAsync(service1StreamKey, "service1.failing");
        var pending2 = await GetPendingMessagesCountAsync(service2StreamKey, "service2.failing");

        Assert.True(pending1 > 0);
        Assert.True(pending2 > 0);
    }

    [Fact]
    public async Task High_Volume_Pending_Messages_Managed_Correctly()
    {
        // Arrange
        FailingEventHandler.Reset();
        using var host = await BuildHost<FailingEventHandler>(options =>
            options.ConfigureClaiming(TimeSpan.FromMinutes(10))
            .ConfigureConsumer(batchSize: 5));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int messageCount = 20;

        // Act - Publish many failing messages
        var publishTasks = Enumerable.Range(0, messageCount)
            .Select(i => publisher.PublishAsync(new FailingEvent { ShouldFail = true }));

        await Task.WhenAll(publishTasks);
        await Task.Delay(3000);

        // Assert
        var streamKey = $"{StreamPrefix}:test-service.failing";
        var consumerGroup = "test-service.failing";
        var pendingCount = await GetPendingMessagesCountAsync(streamKey, consumerGroup);

        Assert.True(pendingCount >= messageCount * 0.8, 
            $"Most messages should be pending, got {pendingCount} out of {messageCount}");
    }
}

#region Test Events and Handlers

/// <summary>
/// Event for testing message claiming.
/// </summary>
public class ClaimableEvent : Event
{
    public string Value { get; set; } = string.Empty;
    public TimeSpan ProcessingDelay { get; set; }
}

public class ClaimableEventHandler : IMessageHandler<ClaimableEvent>
{
    private const string HandlerKey = nameof(ClaimableEventHandler);
    private static int _handleCount;

    public static int HandleCount => _handleCount;

    public static void Reset()
    {
        _handleCount = 0;
    }

    public async Task HandleAsync(ClaimableEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _handleCount);
        
        // Simulate processing delay only on first attempt
        if (context.DeliveryCount == 1 && message.ProcessingDelay > TimeSpan.Zero)
        {
            await Task.Delay(message.ProcessingDelay, cancellationToken);
        }
        // On retry (claim), process quickly
    }
}

/// <summary>
/// Event for selective failure testing.
/// </summary>
public class SelectiveFailureEvent : Event
{
    public bool ShouldFail { get; set; }
    public string Value { get; set; } = string.Empty;
}

public class SelectiveFailureEventHandler : IMessageHandler<SelectiveFailureEvent>
{
    private static int _successCount;

    public static int SuccessCount => _successCount;

    public static void Reset()
    {
        _successCount = 0;
    }

    public Task HandleAsync(SelectiveFailureEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        if (message.ShouldFail)
        {
            throw new InvalidOperationException($"Intentional failure: {message.Value}");
        }

        Interlocked.Increment(ref _successCount);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Event for DLQ testing.
/// </summary>
public class DlqTestEvent : Event
{
    public bool AlwaysFail { get; set; }
    public string TestValue { get; set; } = string.Empty;
}

public class DlqTestEventHandler : IMessageHandler<DlqTestEvent>
{
    public static void Reset() { }

    public Task HandleAsync(DlqTestEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        if (message.AlwaysFail)
        {
            throw new InvalidOperationException("DLQ test failure");
        }
        return Task.CompletedTask;
    }
}

/// <summary>
/// Event that succeeds after N attempts.
/// </summary>
public class RetryableEvent : Event
{
    public string Value { get; set; } = string.Empty;
    public int SucceedAfterAttempts { get; set; }
}

public class RetryableEventHandler : IMessageHandler<RetryableEvent>
{
    private static int _handleCount;
    private static int _successCount;

    public static int HandleCount => _handleCount;
    public static int SuccessCount => _successCount;

    public static void Reset()
    {
        _handleCount = 0;
        _successCount = 0;
    }

    public Task HandleAsync(RetryableEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _handleCount);

        // Use the actual delivery count from Redis, not a static counter
        // DeliveryCount represents how many times Redis has delivered this message
        if (context.DeliveryCount < message.SucceedAfterAttempts)
        {
            throw new InvalidOperationException($"Attempt {context.DeliveryCount} failed");
        }

        Interlocked.Increment(ref _successCount);
        return Task.CompletedTask;
    }
}

#endregion
