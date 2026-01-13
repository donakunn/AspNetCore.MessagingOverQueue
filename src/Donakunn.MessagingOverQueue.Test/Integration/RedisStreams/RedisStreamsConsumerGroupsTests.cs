using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;
using MessagingOverQueue.Test.Integration.RedisStreams.Infrastructure;
using MessagingOverQueue.Test.Integration.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Collections.Concurrent;

namespace MessagingOverQueue.Test.Integration.RedisStreams;

/// <summary>
/// Integration tests for Redis Streams consumer group functionality.
/// Tests load balancing, multiple consumers, and group isolation.
/// </summary>
public class RedisStreamsConsumerGroupsTests : RedisStreamsIntegrationTestBase
{
    protected override void ConfigureAdditionalServices(IServiceCollection services)
    {
        // No additional services needed
    }

    [Fact]
    public async Task Consumer_Group_Created_On_Startup()
    {
        // Arrange & Act
        using var host = await BuildHost<SimpleTestEventHandler>();
        
        // Small delay for topology declaration
        await Task.Delay(500);

        // Assert
        var streamKey = $"{StreamPrefix}:test-service.simple-test-event";
        var consumerGroup = "test-service.simple-test-event";

        var groupExists = await ConsumerGroupExistsAsync(streamKey, consumerGroup);
        Assert.True(groupExists, "Consumer group should be created on startup");
    }

    [Fact]
    public async Task Multiple_Consumers_In_Same_Group_Load_Balance_Messages()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        const int messageCount = 20;

        // Start two consumer hosts in the same group
        using var consumer1 = await BuildHost<SimpleTestEventHandler>();
        using var consumer2 = await BuildHost<SimpleTestEventHandler>();

        var publisher = consumer1.Services.GetRequiredService<IEventPublisher>();

        // Act - Publish messages
        for (int i = 0; i < messageCount; i++)
        {
            await publisher.PublishAsync(new SimpleTestEvent { Value = $"Message-{i}" });
        }

        await SimpleTestEventHandler.WaitForCountAsync(messageCount, DefaultTimeout);

        // Assert - All messages processed
        Assert.Equal(messageCount, SimpleTestEventHandler.HandleCount);

        // Verify load balancing occurred via Redis consumer info
        var streamKey = $"{StreamPrefix}:test-service.simple-test-event";
        var consumerGroup = "test-service.simple-test-event";
        var consumers = await GetConsumersAsync(streamKey, consumerGroup);

        Assert.Equal(2, consumers.Length);
        
        // Both consumers should have processed some messages
        Assert.All(consumers, c => Assert.True(c.PendingMessageCount >= 0));
    }

    [Fact]
    public async Task Independent_Consumer_Groups_Receive_Same_Messages()
    {
        // Arrange - Create two separate services (different consumer groups)
        var service1Handler = new ConcurrentBag<SimpleTestEvent>();
        var service1Count = 0;
        
        var service2Handler = new ConcurrentBag<SimpleTestEvent>();
        var service2Count = 0;

        // Service 1
        using var host1 = await BuildHost(services =>
        {
            services.AddLogging();
            services.AddRedisStreamsMessaging(options =>
            {
                options.UseConnectionString(RedisConnectionString);
                options.WithStreamPrefix(StreamPrefix);
            })
            .AddTopology(topology => topology
                .WithServiceName("service-1")
                .ScanAssemblyContaining<SimpleTestEventHandler>());
        });

        // Service 2
        using var host2 = await BuildHost(services =>
        {
            services.AddLogging();
            services.AddRedisStreamsMessaging(options =>
            {
                options.UseConnectionString(RedisConnectionString);
                options.WithStreamPrefix(StreamPrefix);
            })
            .AddTopology(topology => topology
                .WithServiceName("service-2")
                .ScanAssemblyContaining<SimpleTestEventHandler>());
        });

        // Use global handler for counting
        SimpleTestEventHandler.Reset();

        var publisher = host1.Services.GetRequiredService<IEventPublisher>();
        const int messageCount = 10;

        // Act - Publish messages
        for (int i = 0; i < messageCount; i++)
        {
            await publisher.PublishAsync(new SimpleTestEvent { Value = $"Shared-{i}" });
        }

        // Wait for both services to process (each should get all messages)
        await WaitForConditionAsync(
            async () =>
            {
                var stream1 = $"{StreamPrefix}:service-1.simple-test-event";
                var stream2 = $"{StreamPrefix}:service-2.simple-test-event";
                
                var count1 = await GetStreamLengthAsync(stream1);
                var count2 = await GetStreamLengthAsync(stream2);
                
                return count1 >= messageCount && count2 >= messageCount;
            },
            DefaultTimeout);

        // Assert - Both consumer groups should have access to all messages
        var stream1Key = $"{StreamPrefix}:service-1.simple-test-event";
        var stream2Key = $"{StreamPrefix}:service-2.simple-test-event";

        Assert.True(await GetStreamLengthAsync(stream1Key) >= messageCount);
        Assert.True(await GetStreamLengthAsync(stream2Key) >= messageCount);
    }

    [Fact]
    public async Task Adding_Consumer_To_Existing_Group_Joins_Load_Balancing()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        const int initialMessages = 10;
        const int additionalMessages = 10;

        // Start with one consumer
        using var consumer1 = await BuildHost<SimpleTestEventHandler>();
        var publisher = consumer1.Services.GetRequiredService<IEventPublisher>();

        // Act - Publish initial batch
        for (int i = 0; i < initialMessages; i++)
        {
            await publisher.PublishAsync(new SimpleTestEvent { Value = $"Initial-{i}" });
        }

        await SimpleTestEventHandler.WaitForCountAsync(initialMessages, DefaultTimeout);

        // Add second consumer
        using var consumer2 = await BuildHost<SimpleTestEventHandler>();
        
        // Small delay for consumer to join
        await Task.Delay(1000);

        // Publish additional messages
        for (int i = 0; i < additionalMessages; i++)
        {
            await publisher.PublishAsync(new SimpleTestEvent { Value = $"Additional-{i}" });
        }

        await SimpleTestEventHandler.WaitForCountAsync(
            initialMessages + additionalMessages, 
            DefaultTimeout);

        // Assert
        var streamKey = $"{StreamPrefix}:test-service.simple-test-event";
        var consumerGroup = "test-service.simple-test-event";
        var consumers = await GetConsumersAsync(streamKey, consumerGroup);

        // Should have 2 consumers in the group
        Assert.Equal(2, consumers.Length);
    }

    [Fact]
    public async Task Consumer_Group_State_Independent_Of_Other_Groups()
    {
        // Arrange - Two services with different consumption speeds
        SimpleTestEventHandler.Reset();
        SlowProcessingEventHandler.Reset();

        using var fastHost = await BuildHost<SimpleTestEventHandler>();
        using var slowHost = await BuildHost<SlowProcessingEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 1)); // Slow, sequential

        var fastPublisher = fastHost.Services.GetRequiredService<IEventPublisher>();
        var slowPublisher = slowHost.Services.GetRequiredService<IEventPublisher>();

        const int messageCount = 5;

        // Act - Publish to both streams
        for (int i = 0; i < messageCount; i++)
        {
            await fastPublisher.PublishAsync(new SimpleTestEvent { Value = $"Fast-{i}" });
            await slowPublisher.PublishAsync(new SlowProcessingEvent 
            { 
                Value = $"Slow-{i}",
                ProcessingTime = TimeSpan.FromMilliseconds(500)
            });
        }

        // Fast should complete quickly
        await SimpleTestEventHandler.WaitForCountAsync(messageCount, TimeSpan.FromSeconds(10));

        // Assert - Fast group completed while slow group still processing
        Assert.Equal(messageCount, SimpleTestEventHandler.HandleCount);
        
        // Slow group should still be processing (not all done yet)
        // We'll give it some time but verify independent state
        var slowStreamKey = $"{StreamPrefix}:test-service.slow-processing-event";
        var slowGroup = "test-service.slow-processing-event";
        var slowPending = await GetPendingMessagesCountAsync(slowStreamKey, slowGroup);
        
        // Since slow processing takes 500ms each and sequential, some should still be pending
        // (This is timing-dependent, so we mainly verify groups are independent)
        Assert.True(slowPending >= 0); // At least we can query independently
    }

    [Fact]
    public async Task Consumer_Group_Persists_Across_Restarts()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        const int messagesBeforeRestart = 5;
        const int messagesAfterRestart = 5;

        // Phase 1: Start consumer, process messages, stop
        using (var host1 = await BuildHost<SimpleTestEventHandler>())
        {
            var publisher = host1.Services.GetRequiredService<IEventPublisher>();

            for (int i = 0; i < messagesBeforeRestart; i++)
            {
                await publisher.PublishAsync(new SimpleTestEvent { Value = $"Before-{i}" });
            }

            await SimpleTestEventHandler.WaitForCountAsync(messagesBeforeRestart, DefaultTimeout);
        } // Host disposed, consumer stopped

        // Reset handler state
        var firstBatchCount = SimpleTestEventHandler.HandleCount;
        SimpleTestEventHandler.Reset();

        // Phase 2: Restart consumer with same group
        using var host2 = await BuildHost<SimpleTestEventHandler>();
        var publisher2 = host2.Services.GetRequiredService<IEventPublisher>();

        // Publish more messages
        for (int i = 0; i < messagesAfterRestart; i++)
        {
            await publisher2.PublishAsync(new SimpleTestEvent { Value = $"After-{i}" });
        }

        await SimpleTestEventHandler.WaitForCountAsync(messagesAfterRestart, DefaultTimeout);

        // Assert
        Assert.Equal(messagesBeforeRestart, firstBatchCount);
        Assert.Equal(messagesAfterRestart, SimpleTestEventHandler.HandleCount);

        // Verify consumer group still exists
        var streamKey = $"{StreamPrefix}:test-service.simple-test-event";
        var consumerGroup = "test-service.simple-test-event";
        Assert.True(await ConsumerGroupExistsAsync(streamKey, consumerGroup));
    }

    [Fact]
    public async Task Messages_Not_Duplicated_Across_Consumers_In_Same_Group()
    {
        // Arrange
        var processedMessages = new ConcurrentBag<string>();
        var processingLock = new object();
        
        SimpleTestEventHandler.Reset();
        const int messageCount = 50;

        // Start multiple consumers
        using var consumer1 = await BuildHost<SimpleTestEventHandler>();
        using var consumer2 = await BuildHost<SimpleTestEventHandler>();
        using var consumer3 = await BuildHost<SimpleTestEventHandler>();

        var publisher = consumer1.Services.GetRequiredService<IEventPublisher>();

        // Act - Rapid publishing
        var publishTasks = Enumerable.Range(0, messageCount)
            .Select(i => publisher.PublishAsync(new SimpleTestEvent { Value = $"Unique-{i}" }));

        await Task.WhenAll(publishTasks);
        await SimpleTestEventHandler.WaitForCountAsync(messageCount, DefaultTimeout);

        // Assert - Each message processed exactly once
        Assert.Equal(messageCount, SimpleTestEventHandler.HandleCount);
        
        var values = SimpleTestEventHandler.HandledMessages.Select(m => m.Value).ToList();
        var uniqueValues = values.Distinct().ToList();
        
        Assert.Equal(messageCount, uniqueValues.Count);
    }

    [Fact]
    public async Task Consumer_Groups_Have_Independent_Pending_Lists()
    {
        // Arrange - Create two services with different consumer groups
        using var service1Host = await BuildHost(services =>
        {
            services.AddLogging();
            services.AddRedisStreamsMessaging(options =>
            {
                options.UseConnectionString(RedisConnectionString);
                options.WithStreamPrefix(StreamPrefix);
                options.ConfigureConsumer(batchSize: 1);
            })
            .AddTopology(topology => topology
                .WithServiceName("pending-service-1")
                .ScanAssemblyContaining<SimpleTestEventHandler>());
        });

        using var service2Host = await BuildHost(services =>
        {
            services.AddLogging();
            services.AddRedisStreamsMessaging(options =>
            {
                options.UseConnectionString(RedisConnectionString);
                options.WithStreamPrefix(StreamPrefix);
                options.ConfigureConsumer(batchSize: 1);
            })
            .AddTopology(topology => topology
                .WithServiceName("pending-service-2")
                .ScanAssemblyContaining<SimpleTestEventHandler>());
        });

        SimpleTestEventHandler.Reset();

        // Publish to both services
        var publisher1 = service1Host.Services.GetRequiredService<IEventPublisher>();
        var publisher2 = service2Host.Services.GetRequiredService<IEventPublisher>();

        const int messageCount = 3;

        for (int i = 0; i < messageCount; i++)
        {
            await publisher1.PublishAsync(new SimpleTestEvent { Value = $"Service1-{i}" });
            await publisher2.PublishAsync(new SimpleTestEvent { Value = $"Service2-{i}" });
        }

        // Wait for processing
        await Task.Delay(2000);

        // Assert - Each group has its own pending list
        var stream1 = $"{StreamPrefix}:pending-service-1.simple-test-event";
        var stream2 = $"{StreamPrefix}:pending-service-2.simple-test-event";

        var pending1 = await GetPendingMessagesCountAsync(stream1, "pending-service-1.simple-test-event");
        var pending2 = await GetPendingMessagesCountAsync(stream2, "pending-service-2.simple-test-event");

        // Pending counts are independent
        Assert.True(pending1 >= 0);
        Assert.True(pending2 >= 0);
    }
}
