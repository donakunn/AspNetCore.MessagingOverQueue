using MessagingOverQueue.src.Abstractions.Publishing;
using MessagingOverQueue.src.DependencyInjection;
using MessagingOverQueue.Test.Integration.Infrastructure;
using MessagingOverQueue.Test.Integration.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using static MessagingOverQueue.src.Topology.DependencyInjection.TopologyServiceCollectionExtensions;

namespace MessagingOverQueue.Test.Integration;

/// <summary>
/// Integration tests for event publishing and handling.
/// </summary>
public class EventIntegrationTests : IntegrationTestBase
{
    [Fact]
    public async Task Event_Is_Published_And_Handled_By_Handler()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Hello" });
        await SimpleTestEventHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(10));

        // Assert
        Assert.Equal(1, SimpleTestEventHandler.HandleCount);
        Assert.Single(SimpleTestEventHandler.HandledMessages);
        Assert.Equal("Hello", SimpleTestEventHandler.HandledMessages.First().Value);
    }

    [Fact]
    public async Task Multiple_Events_Are_Handled_Sequentially()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int eventCount = 10;

        // Act
        for (int i = 0; i < eventCount; i++)
        {
            await publisher.PublishAsync(new SimpleTestEvent { Value = $"Message-{i}" });
        }

        await SimpleTestEventHandler.WaitForCountAsync(eventCount, TimeSpan.FromSeconds(30));

        // Assert
        Assert.Equal(eventCount, SimpleTestEventHandler.HandleCount);
        Assert.Equal(eventCount, SimpleTestEventHandler.HandledMessages.Count);
    }

    [Fact]
    public async Task Complex_Event_Payload_Is_Serialized_And_Deserialized_Correctly()
    {
        // Arrange
        ComplexTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        var originalEvent = new ComplexTestEvent
        {
            Name = "TestEvent",
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
        await ComplexTestEventHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(10));

        // Assert
        Assert.Equal(1, ComplexTestEventHandler.HandleCount);
        var handledEvent = ComplexTestEventHandler.HandledMessages.First();
        Assert.Equal("TestEvent", handledEvent.Name);
        Assert.Equal(42, handledEvent.Count);
        Assert.Equal(123.45m, handledEvent.Amount);
        Assert.Equal(3, handledEvent.Tags.Count);
        Assert.NotNull(handledEvent.ProcessedAt);
    }

    [Fact]
    public async Task Event_With_CorrelationId_Is_Preserved()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        var correlationId = Guid.NewGuid().ToString();
        var @event = new SimpleTestEvent { Value = "Correlated" };

        // Set correlation ID through reflection (in real code, use WithCorrelationId)
        var type = typeof(SimpleTestEvent);
        type.GetProperty("CorrelationId")?.SetValue(@event, correlationId);

        // Act
        await publisher.PublishAsync(@event);
        await SimpleTestEventHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(10));

        // Assert
        Assert.Equal(1, SimpleTestEventHandler.HandleCount);
        var handled = SimpleTestEventHandler.HandledMessages.First();
        Assert.Equal(correlationId, handled.CorrelationId);
    }

    [Fact]
    public async Task High_Volume_Events_Are_Processed_Successfully()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int eventCount = 100;

        // Act
        var publishTasks = Enumerable.Range(0, eventCount)
            .Select(i => publisher.PublishAsync(new SimpleTestEvent { Value = $"Bulk-{i}" }));

        await Task.WhenAll(publishTasks);
        await SimpleTestEventHandler.WaitForCountAsync(eventCount, TimeSpan.FromSeconds(60));

        // Assert
        Assert.Equal(eventCount, SimpleTestEventHandler.HandleCount);
    }

    [Fact]
    public async Task Concurrent_Publishing_Is_Thread_Safe()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int concurrentPublishers = 10;
        const int eventsPerPublisher = 20;
        const int totalEvents = concurrentPublishers * eventsPerPublisher;

        // Act
        var publisherTasks = Enumerable.Range(0, concurrentPublishers)
            .Select(publisherIndex => Task.Run(async () =>
            {
                for (int i = 0; i < eventsPerPublisher; i++)
                {
                    await publisher.PublishAsync(new SimpleTestEvent
                    {
                        Value = $"Publisher-{publisherIndex}-Event-{i}"
                    });
                }
            }));

        await Task.WhenAll(publisherTasks);
        await SimpleTestEventHandler.WaitForCountAsync(totalEvents, TimeSpan.FromSeconds(60));

        // Assert
        Assert.Equal(totalEvents, SimpleTestEventHandler.HandleCount);
    }

    private async Task<IHost> BuildHostWithHandlers()
    {
        return await BuildHost(services =>
        {
            services.AddRabbitMqMessaging(options =>
            {
                options.UseHost(_rabbitMqContainer!.Hostname);
                options.UsePort(_rabbitMqContainer!.GetMappedPublicPort(5672));
                options.WithCredentials("guest", "guest");
                options.WithConnectionName($"EventTests-{Guid.NewGuid():N}");
            })
            .AddTopology(topology => topology
                .WithServiceName("event-test-service")
                .ScanAssemblyContaining<SimpleTestEventHandler>());
        });
    }

    protected override void ConfigureAdditionalServices(IServiceCollection services)
    {
        // No additional services needed for these tests
    }
}
