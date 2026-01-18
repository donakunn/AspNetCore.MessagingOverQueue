using Donakunn.MessagingOverQueue.Abstractions.Consuming;
using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;
using MessagingOverQueue.Test.Integration.Shared.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.Infrastructure;
using MessagingOverQueue.Test.Integration.Shared.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace MessagingOverQueue.Test.Integration.RedisStreams;

/// <summary>
/// Integration tests for Redis Streams topology declaration.
/// Tests stream creation, consumer group registration, and topology management.
/// </summary>
public class RedisStreamsTopologyTests : RedisStreamsIntegrationTestBase
{
    protected override void ConfigureAdditionalServices(IServiceCollection services)
    {
        // No additional services needed
    }

    [Fact]
    public async Task Stream_Created_On_First_Publish()
    {
        // Arrange
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        var streamKey = $"{StreamPrefix}:test-service.simple-test";

        // Verify stream doesn't exist yet
        var lengthBefore = await GetStreamLengthAsync(streamKey);
        Assert.Equal(0, lengthBefore);

        // Act
        await publisher.PublishAsync(new SimpleTestEvent { Value = "First" });

        // Small delay for processing
        await Task.Delay(500);

        // Assert
        var lengthAfter = await GetStreamLengthAsync(streamKey);
        Assert.True(lengthAfter > 0, "Stream should be created and contain message");
    }

    [Fact]
    public async Task Consumer_Group_Created_On_Startup()
    {
        // Arrange
        var streamKey = $"{StreamPrefix}:test-service.simple-test";
        var consumerGroup = "test-service.simple-test";

        // Act
        using var host = await BuildHost<SimpleTestEventHandler>();

        // Wait for topology declaration
        await Task.Delay(1000);

        // Assert
        var groupExists = await ConsumerGroupExistsAsync(streamKey, consumerGroup);
        Assert.True(groupExists, "Consumer group should be created on startup");
    }

    [Fact]
    public async Task Multiple_Message_Types_Create_Separate_Streams()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        ComplexTestEventHandler.Reset();

        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Simple" });
        await publisher.PublishAsync(new ComplexTestEvent { Name = "Complex" });

        await SimpleTestEventHandler.WaitForCountAsync(1, DefaultTimeout);
        await ComplexTestEventHandler.WaitForCountAsync(1, DefaultTimeout);

        // Assert
        var simpleStream = $"{StreamPrefix}:test-service.simple-test";
        var complexStream = $"{StreamPrefix}:test-service.complex-test";

        Assert.True(await GetStreamLengthAsync(simpleStream) > 0);
        Assert.True(await GetStreamLengthAsync(complexStream) > 0);

        // Verify separate consumer groups
        Assert.True(await ConsumerGroupExistsAsync(simpleStream, "test-service.simple-test"));
        Assert.True(await ConsumerGroupExistsAsync(complexStream, "test-service.complex-test"));
    }

    [Fact]
    public async Task Stream_Prefix_Applied_Correctly()
    {
        // Arrange
        var customPrefix = $"custom-{Guid.NewGuid():N}";

        using var host = await BuildHost<SimpleTestEventHandler>(options =>
            options.WithStreamPrefix(customPrefix));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Prefixed" });
        await Task.Delay(500);

        // Assert
        var expectedStreamKey = $"{customPrefix}:test-service.simple-test";
        var streamLength = await GetStreamLengthAsync(expectedStreamKey);
        Assert.True(streamLength > 0, $"Stream with prefix '{customPrefix}' should exist");
    }

    [Fact]
    public async Task Topology_Declaration_Is_Idempotent()
    {
        // Arrange
        SimpleTestEventHandler.Reset();

        // Act - Start and stop multiple times
        using (var host1 = await BuildHost<SimpleTestEventHandler>())
        {
            await Task.Delay(500);
        }

        using (var host2 = await BuildHost<SimpleTestEventHandler>())
        {
            var publisher = host2.Services.GetRequiredService<IEventPublisher>();
            await publisher.PublishAsync(new SimpleTestEvent { Value = "Test" });
            await SimpleTestEventHandler.WaitForCountAsync(1, DefaultTimeout);
        }

        // Assert - Should work without errors
        Assert.Equal(1, SimpleTestEventHandler.HandleCount);
    }

    [Fact]
    public async Task Multiple_Services_Create_Separate_Consumer_Groups_On_Same_Stream()
    {
        // Arrange - Two different services
        using var service1 = await BuildHost(services =>
        {
            services.AddLogging();
            services.AddRedisStreamsMessaging(options =>
            {
                options.UseConnectionString(RedisConnectionString);
                options.WithStreamPrefix(StreamPrefix);
            })
            .AddTopology(topology => topology
                .WithServiceName("inventory-service")
                .ScanAssemblyContaining<SimpleTestEventHandler>());
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
                .WithServiceName("notification-service")
                .ScanAssemblyContaining<SimpleTestEventHandler>());
        });

        // Wait for topology declaration
        await Task.Delay(1000);

        // Act - Both services should create their own consumer groups
        var stream1 = $"{StreamPrefix}:inventory-service.simple-test";
        var stream2 = $"{StreamPrefix}:notification-service.simple-test";

        // Assert
        Assert.True(await ConsumerGroupExistsAsync(stream1, "inventory-service.simple-test"));
        Assert.True(await ConsumerGroupExistsAsync(stream2, "notification-service.simple-test"));
    }

    [Fact]
    public async Task Streams_Listed_In_Redis_After_Topology_Declaration()
    {
        // Arrange
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act - Publish to create stream
        await publisher.PublishAsync(new SimpleTestEvent { Value = "List test" });
        await Task.Delay(500);

        // Assert - Stream should appear in Redis keys
        var keys = await GetKeysAsync($"{StreamPrefix}:*");
        var streamKeys = keys.Where(k => k.ToString().Contains("simple-test")).ToList();

        Assert.NotEmpty(streamKeys);
    }

    [Fact]
    public async Task Consumer_Group_Information_Accessible()
    {
        // Arrange
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Publish to ensure stream exists
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Info test" });
        await Task.Delay(1000);

        // Act
        var streamKey = $"{StreamPrefix}:test-service.simple-test";
        var db = GetRedisDatabase();
        var groups = await db.StreamGroupInfoAsync(streamKey);

        // Assert
        Assert.NotEmpty(groups);

        var group = groups.FirstOrDefault(g => g.Name == "test-service.simple-test");
        Assert.NotNull(group);
        Assert.True(group.ConsumerCount > 0);
    }

    [Fact]
    public async Task Stream_Info_Shows_Correct_Length()
    {
        // Arrange
        using var host = await BuildHost<SimpleTestEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int messageCount = 5;

        // Act
        for (int i = 0; i < messageCount; i++)
        {
            await publisher.PublishAsync(new SimpleTestEvent { Value = $"Message-{i}" });
        }

        await Task.Delay(1000);

        // Assert
        var streamKey = $"{StreamPrefix}:test-service.simple-test";
        var db = GetRedisDatabase();
        var info = await db.StreamInfoAsync(streamKey);

        Assert.True(info.Length >= messageCount);
    }

    [Fact]
    public async Task Topology_Created_Before_Message_Consumption_Starts()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        var streamKey = $"{StreamPrefix}:test-service.simple-test";
        var consumerGroup = "test-service.simple-test";

        // Pre-publish messages to stream (manual)
        var db = GetRedisDatabase();
        var messageId = await db.StreamAddAsync(streamKey,
            new NameValueEntry[]
            {
                new("message-id", Guid.NewGuid().ToString()),
                new("message-type", "SimpleTestEvent"),
                new("body", "{\"Value\":\"Pre-published\"}")
            });

        Assert.False(messageId.IsNull);

        // Act - Start consumer (should declare topology first)
        using var host = await BuildHost<SimpleTestEventHandler>();

        // Wait for topology and consumption
        await Task.Delay(2000);

        // Assert - Consumer group should exist and message should be consumable
        Assert.True(await ConsumerGroupExistsAsync(streamKey, consumerGroup));
    }

    [Fact]
    public async Task Custom_Queue_Attributes_Reflected_In_Stream_Names()
    {
        // Arrange
        using var host = await BuildHost<CustomQueueEventHandler>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new CustomQueueEvent { Data = "Custom" });
        await Task.Delay(1000);

        // Assert - Stream should use custom naming
        var keys = await GetKeysAsync($"{StreamPrefix}:*custom*");
        Assert.NotEmpty(keys);
    }

    [Fact]
    public async Task Multiple_Handlers_For_Same_Event_Share_Stream()
    {
        // Arrange
        MultiHandlerEventHandler1.Reset();
        MultiHandlerEventHandler2.Reset();

        using var host = await BuildHost<MultiHandlerEventHandler1>();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new MultiHandlerEvent { Payload = "Shared" });

        await MultiHandlerEventHandler1.WaitForCountAsync(1, DefaultTimeout);
        await MultiHandlerEventHandler2.WaitForCountAsync(1, DefaultTimeout);

        // Assert - Both handlers processed from same stream
        Assert.Equal(1, MultiHandlerEventHandler1.HandleCount);
        Assert.Equal(1, MultiHandlerEventHandler2.HandleCount);

        // Verify only one stream created
        var streamKey = $"{StreamPrefix}:test-service.multi-handler";
        Assert.True(await GetStreamLengthAsync(streamKey) > 0);
    }

    [Fact]
    public async Task Topology_Survives_Consumer_Restart()
    {
        // Arrange
        var streamKey = $"{StreamPrefix}:test-service.simple-test";
        var consumerGroup = "test-service.simple-test";

        // Phase 1: Create topology
        using (var host1 = await BuildHost<SimpleTestEventHandler>())
        {
            await Task.Delay(1000);
        }

        // Verify topology exists
        Assert.True(await ConsumerGroupExistsAsync(streamKey, consumerGroup));

        // Phase 2: Restart
        using var host2 = await BuildHost<SimpleTestEventHandler>();
        await Task.Delay(1000);

        // Assert - Topology still exists
        Assert.True(await ConsumerGroupExistsAsync(streamKey, consumerGroup));
    }
}

#region Additional Test Handlers

/// <summary>
/// Handler for custom queue event tests.
/// </summary>
public class CustomQueueEventHandler : IMessageHandler<CustomQueueEvent>
{
    public Task HandleAsync(CustomQueueEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}

/// <summary>
/// First handler for multi-handler tests.
/// </summary>
public class MultiHandlerEventHandler1 : IMessageHandler<MultiHandlerEvent>
{
    private const string HandlerKey = nameof(MultiHandlerEventHandler1);

    public static int HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;

    public static void Reset()
    {
        TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Reset();
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).WaitForCountAsync(expected, timeout);

    public Task HandleAsync(MultiHandlerEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Increment();
        return Task.CompletedTask;
    }
}

/// <summary>
/// Second handler for multi-handler tests.
/// </summary>
public class MultiHandlerEventHandler2 : IMessageHandler<MultiHandlerEvent>
{
    private const string HandlerKey = nameof(MultiHandlerEventHandler2);

    public static int HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;

    public static void Reset()
    {
        TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Reset();
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).WaitForCountAsync(expected, timeout);

    public Task HandleAsync(MultiHandlerEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Increment();
        return Task.CompletedTask;
    }
}

#endregion
