using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection;
using MessagingOverQueue.Test.Integration.RedisStreams.Infrastructure;
using MessagingOverQueue.Test.Integration.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace MessagingOverQueue.Test.Integration.RedisStreams;

/// <summary>
/// End-to-end integration tests for Redis Streams provider.
/// Tests the complete flow from publishing to consumption.
/// </summary>
public class RedisStreamsEndToEndTests : RedisStreamsIntegrationTestBase
{
    protected override void ConfigureAdditionalServices(IServiceCollection services)
    {
        // No additional services needed for basic tests
    }

    [Fact]
    public async Task Message_Published_And_Consumed_Successfully()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Hello Redis Streams" });
        await SimpleTestEventHandler.WaitForCountAsync(1, DefaultTimeout);

        // Assert
        Assert.Equal(1, SimpleTestEventHandler.HandleCount);
        var handledMessage = SimpleTestEventHandler.HandledMessages.First();
        Assert.Equal("Hello Redis Streams", handledMessage.Value);
    }

    [Fact]
    public async Task Multiple_Messages_Processed_In_Order()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int messageCount = 20;

        // Act
        for (int i = 0; i < messageCount; i++)
        {
            await publisher.PublishAsync(new SimpleTestEvent { Value = $"Message-{i}" });
        }

        await SimpleTestEventHandler.WaitForCountAsync(messageCount, DefaultTimeout);

        // Assert
        Assert.Equal(messageCount, SimpleTestEventHandler.HandleCount);
        Assert.Equal(messageCount, SimpleTestEventHandler.HandledMessages.Count);
        
        // Verify order preservation
        var messages = SimpleTestEventHandler.HandledMessages.ToList();
        for (int i = 0; i < messageCount; i++)
        {
            Assert.Equal($"Message-{i}", messages[i].Value);
        }
    }

    [Fact]
    public async Task Complex_Payload_Serialized_And_Deserialized_Correctly()
    {
        // Arrange
        ComplexTestEventHandler.Reset();
        using var host = await BuildHost<ComplexTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        var originalEvent = new ComplexTestEvent
        {
            Name = "ComplexEvent",
            Count = 42,
            Amount = 123.45m,
            Tags = ["tag1", "tag2", "tag3"],
            Metadata = new Dictionary<string, object>
            {
                ["key1"] = "value1",
                ["key2"] = 100
            }
        };

        // Act
        await publisher.PublishAsync(originalEvent);
        await ComplexTestEventHandler.WaitForCountAsync(1, DefaultTimeout);

        // Assert
        var handledEvent = ComplexTestEventHandler.HandledMessages.First();
        Assert.Equal("ComplexEvent", handledEvent.Name);
        Assert.Equal(42, handledEvent.Count);
        Assert.Equal(123.45m, handledEvent.Amount);
        Assert.Equal(3, handledEvent.Tags.Count);
        Assert.Contains("tag1", handledEvent.Tags);
        Assert.NotNull(handledEvent.ProcessedAt);
    }

    [Fact]
    public async Task High_Volume_Messages_Processed_Successfully()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int messageCount = 100;

        // Act - Publish in parallel
        var publishTasks = Enumerable.Range(0, messageCount)
            .Select(i => publisher.PublishAsync(new SimpleTestEvent { Value = $"Bulk-{i}" }));

        await Task.WhenAll(publishTasks);
        await SimpleTestEventHandler.WaitForCountAsync(messageCount, TimeSpan.FromSeconds(60));

        // Assert
        Assert.Equal(messageCount, SimpleTestEventHandler.HandleCount);
    }

    [Fact]
    public async Task Message_With_CorrelationId_Preserved_Through_Pipeline()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        var correlationId = Guid.NewGuid().ToString();
        var @event = new SimpleTestEvent { Value = "Correlated" };
        
        // Set correlation ID
        var eventType = typeof(SimpleTestEvent);
        eventType.GetProperty("CorrelationId")?.SetValue(@event, correlationId);

        // Act
        await publisher.PublishAsync(@event);
        await SimpleTestEventHandler.WaitForCountAsync(1, DefaultTimeout);

        // Assert
        var handled = SimpleTestEventHandler.HandledMessages.First();
        Assert.Equal(correlationId, handled.CorrelationId);
    }

    [Fact]
    public async Task Concurrent_Publishing_Is_Thread_Safe()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int concurrentPublishers = 10;
        const int messagesPerPublisher = 20;
        const int totalMessages = concurrentPublishers * messagesPerPublisher;

        // Act - Concurrent publishing from multiple threads
        var publisherTasks = Enumerable.Range(0, concurrentPublishers)
            .Select(publisherIndex => Task.Run(async () =>
            {
                for (int i = 0; i < messagesPerPublisher; i++)
                {
                    await publisher.PublishAsync(new SimpleTestEvent 
                    { 
                        Value = $"Publisher-{publisherIndex}-Message-{i}" 
                    });
                }
            }));

        await Task.WhenAll(publisherTasks);
        await SimpleTestEventHandler.WaitForCountAsync(totalMessages, TimeSpan.FromSeconds(60));

        // Assert
        Assert.Equal(totalMessages, SimpleTestEventHandler.HandleCount);
    }

    [Fact]
    public async Task Messages_Stored_In_Redis_Stream_Correctly()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Test" });
        await SimpleTestEventHandler.WaitForCountAsync(1, DefaultTimeout);

        // Assert - Verify stream exists in Redis
        var streamKey = $"{StreamPrefix}:test-service.simple-test";
        var streamLength = await GetStreamLengthAsync(streamKey);
        Assert.True(streamLength > 0, "Stream should contain at least one message");

        // Verify message structure
        var messages = await GetStreamMessagesAsync(streamKey);
        Assert.NotEmpty(messages);
        
        var message = messages[0];
        var fields = message.Values.ToDictionary(v => v.Name.ToString(), v => v.Value.ToString());
        
        Assert.True(fields.ContainsKey("message-id"));
        Assert.True(fields.ContainsKey("message-type"));
        Assert.True(fields.ContainsKey("body"));
    }

    [Fact]
    public async Task Multiple_Event_Types_Published_To_Different_Streams()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        ComplexTestEventHandler.Reset();
        
        using var host = await BuildHost<SimpleTestEventHandler>(options => 
            options.WithStreamPrefix(StreamPrefix));
        
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Simple" });
        await publisher.PublishAsync(new ComplexTestEvent { Name = "Complex" });

        await SimpleTestEventHandler.WaitForCountAsync(1, DefaultTimeout);
        await ComplexTestEventHandler.WaitForCountAsync(1, DefaultTimeout);

        // Assert - Verify separate streams
        var simpleStream = $"{StreamPrefix}:test-service.simple-test";
        var complexStream = $"{StreamPrefix}:test-service.complex-test";

        Assert.True(await GetStreamLengthAsync(simpleStream) > 0);
        Assert.True(await GetStreamLengthAsync(complexStream) > 0);
    }

    [Fact]
    public async Task Slow_Consumer_Does_Not_Block_Other_Messages()
    {
        // Arrange
        SlowProcessingEventHandler.Reset();
        using var host = await BuildHost<SlowProcessingEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 10));
        
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int messageCount = 10;

        // Act - Publish slow processing messages
        var publishTasks = Enumerable.Range(0, messageCount)
            .Select(i => publisher.PublishAsync(new SlowProcessingEvent 
            { 
                Value = $"Slow-{i}",
                ProcessingTime = TimeSpan.FromMilliseconds(200)
            }));

        await Task.WhenAll(publishTasks);
        
        // Should process concurrently, so total time < sequential time
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await SlowProcessingEventHandler.WaitForCountAsync(messageCount, TimeSpan.FromSeconds(30));
        sw.Stop();

        // Assert - With 5 concurrent consumers and 200ms each, 10 messages should take ~400ms
        // Sequential would take 2000ms, so verify it's much faster
        Assert.True(sw.ElapsedMilliseconds < 1500, 
            $"Processing took {sw.ElapsedMilliseconds}ms, expected concurrent processing to be faster");
        Assert.Equal(messageCount, SlowProcessingEventHandler.HandleCount);
    }

    [Fact]
    public async Task Message_Timestamp_Preserved()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        var beforePublish = DateTime.UtcNow;

        // Act
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Timestamped" });
        await SimpleTestEventHandler.WaitForCountAsync(1, DefaultTimeout);

        var afterPublish = DateTime.UtcNow;

        // Assert
        var handled = SimpleTestEventHandler.HandledMessages.First();
        Assert.True(handled.Timestamp >= beforePublish && handled.Timestamp <= afterPublish,
            $"Timestamp {handled.Timestamp} should be between {beforePublish} and {afterPublish}");
    }

    [Fact]
    public async Task Consumer_Starts_After_Publisher_And_Receives_Messages()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        
        // Start host WITHOUT consumer to publish first
        using var publisherHost = await BuildHost(services =>
        {
            services.AddLogging();
            services.AddRedisStreamsMessaging(options =>
            {
                options.UseConnectionString(RedisConnectionString);
                options.WithStreamPrefix(StreamPrefix);
            });
        });

        var publisher = publisherHost.Services.GetRequiredService<IEventPublisher>();

        // Act - Publish before consumer starts
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Pre-consumer" });
        
        // Small delay to ensure message is in stream
        await Task.Delay(500);

        // Now start consumer
        using var consumerHost = await BuildHost<SimpleTestEventHandler>();

        // Wait for message to be consumed
        await SimpleTestEventHandler.WaitForCountAsync(1, DefaultTimeout);

        // Assert
        Assert.Equal(1, SimpleTestEventHandler.HandleCount);
        Assert.Equal("Pre-consumer", SimpleTestEventHandler.HandledMessages.First().Value);
    }
}
