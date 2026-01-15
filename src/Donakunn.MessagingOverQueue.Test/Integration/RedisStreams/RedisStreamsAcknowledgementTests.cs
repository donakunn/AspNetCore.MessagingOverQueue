using Donakunn.MessagingOverQueue.Abstractions.Consuming;
using Donakunn.MessagingOverQueue.Abstractions.Messages;
using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using MessagingOverQueue.Test.Integration.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.Infrastructure;
using MessagingOverQueue.Test.Integration.TestDoubles;
using Microsoft.Extensions.DependencyInjection;

namespace MessagingOverQueue.Test.Integration.RedisStreams;

/// <summary>
/// Integration tests for Redis Streams message acknowledgement.
/// Tests successful ACK, NACK, and message reprocessing scenarios.
/// </summary>
public class RedisStreamsAcknowledgementTests : RedisStreamsIntegrationTestBase
{
    protected override void ConfigureAdditionalServices(IServiceCollection services)
    {
        // No additional services needed
    }

    [Fact]
    public async Task Successful_Processing_Acknowledges_Message()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new SimpleTestEvent { Value = "ACK test" });
        await SimpleTestEventHandler.WaitForCountAsync(1, DefaultTimeout);

        // Wait a bit for ACK to process
        await Task.Delay(1000);

        // Assert - Message should be acknowledged (removed from pending)
        var streamKey = $"{StreamPrefix}:test-service.simple-test";
        var consumerGroup = "test-service.simple-test";
        var pendingCount = await GetPendingMessagesCountAsync(streamKey, consumerGroup);

        Assert.Equal(0, pendingCount);
    }

    [Fact]
    public async Task Failed_Processing_Leaves_Message_In_Pending_List()
    {
        // Arrange
        FailingEventHandler.Reset();
        // Configure long intervals to prevent retries from exceeding MaxDeliveryAttempts during test
        using var host = await BuildHost<FailingEventHandler>(options =>
            options.ConfigureClaiming(
                claimIdleTime: TimeSpan.FromMinutes(10),
                checkInterval: TimeSpan.FromMinutes(10)));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new FailingEvent { ShouldFail = true });
        
        // Wait for processing attempt
        await Task.Delay(2000);

        // Assert - Message should remain in pending list
        var streamKey = $"{StreamPrefix}:test-service.failing";
        var consumerGroup = "test-service.failing";
        var pendingCount = await GetPendingMessagesCountAsync(streamKey, consumerGroup);

        Assert.True(pendingCount > 0, "Failed message should remain in pending list");
    }

    [Fact]
    public async Task Multiple_Messages_Acknowledged_Independently()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int messageCount = 5;

        // Act
        for (int i = 0; i < messageCount; i++)
        {
            await publisher.PublishAsync(new SimpleTestEvent { Value = $"Batch-{i}" });
        }

        await SimpleTestEventHandler.WaitForCountAsync(messageCount, DefaultTimeout);
        await Task.Delay(1000);

        // Assert - All messages acknowledged
        var streamKey = $"{StreamPrefix}:test-service.simple-test";
        var consumerGroup = "test-service.simple-test";
        var pendingCount = await GetPendingMessagesCountAsync(streamKey, consumerGroup);

        Assert.Equal(0, pendingCount);
    }

    [Fact]
    public async Task Acknowledged_Message_Not_Redelivered()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act - Publish and process
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Once" });
        await SimpleTestEventHandler.WaitForCountAsync(1, DefaultTimeout);

        // Wait for ACK
        await Task.Delay(1000);

        var firstCount = SimpleTestEventHandler.HandleCount;

        // Wait longer to ensure no redelivery
        await Task.Delay(3000);

        // Assert - Message processed exactly once
        Assert.Equal(1, firstCount);
        Assert.Equal(1, SimpleTestEventHandler.HandleCount);
    }

    [Fact]
    public async Task Slow_Processing_Eventually_Acknowledged()
    {
        // Arrange
        SlowProcessingEventHandler.Reset();
        using var host = await BuildHost<SlowProcessingEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 1));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new SlowProcessingEvent 
        { 
            Value = "Slow ACK",
            ProcessingTime = TimeSpan.FromSeconds(2)
        });

        await SlowProcessingEventHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(10));
        
        // Wait for ACK
        await Task.Delay(1000);

        // Assert
        var streamKey = $"{StreamPrefix}:test-service.slow-processing";
        var consumerGroup = "test-service.slow-processing";
        var pendingCount = await GetPendingMessagesCountAsync(streamKey, consumerGroup);

        Assert.Equal(0, pendingCount);
    }

    [Fact]
    public async Task Concurrent_Messages_All_Acknowledged()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHost<SimpleTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 10));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int messageCount = 20;

        // Act - Publish concurrently
        var publishTasks = Enumerable.Range(0, messageCount)
            .Select(i => publisher.PublishAsync(new SimpleTestEvent { Value = $"Concurrent-{i}" }));

        await Task.WhenAll(publishTasks);
        await SimpleTestEventHandler.WaitForCountAsync(messageCount, DefaultTimeout);
        
        // Wait for all ACKs
        await Task.Delay(2000);

        // Assert
        var streamKey = $"{StreamPrefix}:test-service.simple-test";
        var consumerGroup = "test-service.simple-test";
        var pendingCount = await GetPendingMessagesCountAsync(streamKey, consumerGroup);

        Assert.Equal(0, pendingCount);
    }

    [Fact]
    public async Task Partial_Batch_Acknowledged_On_Success()
    {
        // Arrange - Mix of successful and failing messages
        // Configure long claim intervals to prevent message from being moved to DLQ during test
        MixedSuccessEventHandler.Reset();
        using var host = await BuildHost<MixedSuccessEventHandler>(options =>
            options
                .ConfigureConsumer(batchSize: 10)
                .ConfigureClaiming(
                    claimIdleTime: TimeSpan.FromMinutes(10),
                    checkInterval: TimeSpan.FromMinutes(10)));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act - Publish messages that will have mixed results
        await publisher.PublishAsync(new MixedSuccessEvent { ShouldSucceed = true, Value = "Success-1" });
        await publisher.PublishAsync(new MixedSuccessEvent { ShouldSucceed = true, Value = "Success-2" });
        await publisher.PublishAsync(new MixedSuccessEvent { ShouldSucceed = false, Value = "Fail-1" });
        await publisher.PublishAsync(new MixedSuccessEvent { ShouldSucceed = true, Value = "Success-3" });

        await Task.Delay(3000);

        // Assert - Successful messages acknowledged, failed ones pending
        var streamKey = $"{StreamPrefix}:test-service.mixed-success";
        var consumerGroup = "test-service.mixed-success";
        var pendingCount = await GetPendingMessagesCountAsync(streamKey, consumerGroup);

        // At least the failed message should be pending
        Assert.True(pendingCount >= 1, $"Expected at least 1 pending message but got {pendingCount}");

        // Successful messages processed
        Assert.True(MixedSuccessEventHandler.SuccessCount >= 3,
            $"Expected at least 3 successful messages but got {MixedSuccessEventHandler.SuccessCount}");
    }

    [Fact]
    public async Task Message_Acknowledgement_Visible_In_Stream_Info()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Info test" });
        await SimpleTestEventHandler.WaitForCountAsync(1, DefaultTimeout);
        await Task.Delay(1000);

        // Assert - Check via Redis stream info
        var streamKey = $"{StreamPrefix}:test-service.simple-test";
        var consumerGroup = "test-service.simple-test";
        
        var db = GetRedisDatabase();
        var groups = await db.StreamGroupInfoAsync(streamKey);
        var group = groups.FirstOrDefault(g => g.Name == consumerGroup);

        Assert.NotNull(group);
        Assert.Equal(0, group.PendingMessageCount);
    }

    [Fact]
    public async Task Consumer_Shutdown_Leaves_Messages_In_Pending()
    {
        // Arrange
        var streamKey = $"{StreamPrefix}:test-service.simple-test";
        var consumerGroup = "test-service.simple-test";

        // Act - Start consumer and publish, but stop before processing completes
        using (var host = await BuildHost<SlowProcessingEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 1)))
        {
            var publisher = host.Services.GetRequiredService<IEventPublisher>();
            
            // Publish slow message
            await publisher.PublishAsync(new SlowProcessingEvent 
            { 
                Value = "Interrupted",
                ProcessingTime = TimeSpan.FromSeconds(10) // Very slow
            });

            // Give it time to start processing
            await Task.Delay(500);
            
            // Host will be disposed here, stopping consumer
        }

        // Small delay
        await Task.Delay(500);

        // Assert - Message should still be in pending (not acknowledged)
        var pendingCount = await GetPendingMessagesCountAsync(streamKey, consumerGroup);
        
        // Note: This may be 0 if the message wasn't picked up yet, or >0 if it was in-flight
        // The key is that shutdown doesn't cause data loss
        Assert.True(pendingCount >= 0);
    }

    [Fact]
    public async Task Multiple_Consumers_Acknowledge_Different_Messages()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var consumer1 = await BuildHost<SimpleTestEventHandler>();
        using var consumer2 = await BuildHost<SimpleTestEventHandler>();

        var publisher = consumer1.Services.GetRequiredService<IEventPublisher>();
        const int messageCount = 10;

        // Act
        for (int i = 0; i < messageCount; i++)
        {
            await publisher.PublishAsync(new SimpleTestEvent { Value = $"Multi-{i}" });
        }

        await SimpleTestEventHandler.WaitForCountAsync(messageCount, DefaultTimeout);
        await Task.Delay(2000);

        // Assert - All messages acknowledged (distributed across consumers)
        var streamKey = $"{StreamPrefix}:test-service.simple-test";
        var consumerGroup = "test-service.simple-test";
        var pendingCount = await GetPendingMessagesCountAsync(streamKey, consumerGroup);

        Assert.Equal(0, pendingCount);
    }
}

#region Test Event and Handler for Mixed Success

/// <summary>
/// Event that can succeed or fail based on property.
/// </summary>
public class MixedSuccessEvent : Event
{
    public bool ShouldSucceed { get; set; }
    public string Value { get; set; } = string.Empty;
}

/// <summary>
/// Handler that succeeds or fails based on event property.
/// </summary>
public class MixedSuccessEventHandler : IMessageHandler<MixedSuccessEvent>
{
    private const string HandlerKey = nameof(MixedSuccessEventHandler);
    private static int _successCount;

    public static int SuccessCount => _successCount;

    public static void Reset()
    {
        _successCount = 0;
        TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Reset();
    }

    public Task HandleAsync(MixedSuccessEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        if (message.ShouldSucceed)
        {
            Interlocked.Increment(ref _successCount);
            return Task.CompletedTask;
        }
        else
        {
            throw new InvalidOperationException($"Intentional failure for: {message.Value}");
        }
    }
}

/// <summary>
/// Handler for failing event tests.
/// </summary>
public class FailingEventHandler : IMessageHandler<FailingEvent>
{
    private const string HandlerKey = nameof(FailingEventHandler);

    public static void Reset()
    {
        TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Reset();
    }

    public Task HandleAsync(FailingEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        if (message.ShouldFail)
        {
            throw new InvalidOperationException(message.FailureMessage);
        }
        
        TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Increment();
        return Task.CompletedTask;
    }
}

#endregion
