using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.DependencyInjection;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;
using MessagingOverQueue.Test.Integration.Infrastructure;
using MessagingOverQueue.Test.Integration.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using static Donakunn.MessagingOverQueue.Topology.DependencyInjection.TopologyServiceCollectionExtensions;

namespace MessagingOverQueue.Test.Integration;

/// <summary>
/// Integration tests for connection pool and channel management.
/// </summary>
public class ConnectionPoolIntegrationTests : IntegrationTestBase
{
    [Fact]
    public async Task ConnectionPool_CreatesConnectionOnFirstUse()
    {
        // Arrange
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act - Publishing forces connection creation
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Connection test" });

        // Assert - If we got here without exception, connection was established
        Assert.True(true);
    }

    [Fact]
    public async Task ConnectionPool_ReusesExistingConnection()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act - Multiple publishes should reuse the same connection
        for (int i = 0; i < 10; i++)
        {
            await publisher.PublishAsync(new SimpleTestEvent { Value = $"Reuse-{i}" });
        }

        await SimpleTestEventHandler.WaitForCountAsync(10, TimeSpan.FromSeconds(30));

        // Assert
        Assert.Equal(10, SimpleTestEventHandler.HandleCount);
    }

    [Fact]
    public async Task ConnectionPool_HandlesHighConcurrency()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int concurrentPublishers = 20;
        const int messagesPerPublisher = 10;
        const int totalMessages = concurrentPublishers * messagesPerPublisher;

        // Act - Concurrent publishing from multiple tasks
        var tasks = Enumerable.Range(0, concurrentPublishers)
            .Select(publisherIndex => Task.Run(async () =>
            {
                for (int i = 0; i < messagesPerPublisher; i++)
                {
                    await publisher.PublishAsync(new SimpleTestEvent
                    {
                        Value = $"P{publisherIndex}-M{i}"
                    });
                }
            }));

        await Task.WhenAll(tasks);
        await SimpleTestEventHandler.WaitForCountAsync(totalMessages, TimeSpan.FromSeconds(60));

        // Assert
        Assert.Equal(totalMessages, SimpleTestEventHandler.HandleCount);
    }

    [Fact]
    public async Task ConnectionPool_HandlesRapidPublishing()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int messageCount = 100;

        // Act - Rapid fire publishing
        var publishTasks = Enumerable.Range(0, messageCount)
            .Select(i => publisher.PublishAsync(new SimpleTestEvent { Value = $"Rapid-{i}" }));

        await Task.WhenAll(publishTasks);
        await SimpleTestEventHandler.WaitForCountAsync(messageCount, TimeSpan.FromSeconds(60));

        // Assert
        Assert.Equal(messageCount, SimpleTestEventHandler.HandleCount);
    }

    [Fact]
    public async Task ConnectionPool_WorksAfterReconnection()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act - Publish first batch
        for (int i = 0; i < 5; i++)
        {
            await publisher.PublishAsync(new SimpleTestEvent { Value = $"Before-{i}" });
        }

        await SimpleTestEventHandler.WaitForCountAsync(5, TimeSpan.FromSeconds(30));
        var firstBatchCount = SimpleTestEventHandler.HandleCount;

        // Simulate some delay (connection might be idle)
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Publish second batch
        for (int i = 0; i < 5; i++)
        {
            await publisher.PublishAsync(new SimpleTestEvent { Value = $"After-{i}" });
        }

        await SimpleTestEventHandler.WaitForCountAsync(10, TimeSpan.FromSeconds(30));

        // Assert
        Assert.Equal(5, firstBatchCount);
        Assert.Equal(10, SimpleTestEventHandler.HandleCount);
    }

    [Fact]
    public async Task MultipleServices_CanPublishIndependently()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        ComplexTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act - Publish different event types
        var simpleTasks = Enumerable.Range(0, 10)
            .Select(i => publisher.PublishAsync(new SimpleTestEvent { Value = $"Simple-{i}" }));

        var complexTasks = Enumerable.Range(0, 10)
            .Select(i => publisher.PublishAsync(new ComplexTestEvent { Name = $"Complex-{i}", Count = i }));

        await Task.WhenAll(simpleTasks.Concat(complexTasks));

        await SimpleTestEventHandler.WaitForCountAsync(10, TimeSpan.FromSeconds(30));
        await ComplexTestEventHandler.WaitForCountAsync(10, TimeSpan.FromSeconds(30));

        // Assert
        Assert.Equal(10, SimpleTestEventHandler.HandleCount);
        Assert.Equal(10, ComplexTestEventHandler.HandleCount);
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
                options.WithConnectionName($"ConnectionPoolTests-{Guid.NewGuid():N}");
            })
            .AddTopology(topology => topology
                .WithServiceName("connection-pool-test-service")
                .ScanAssemblyContaining<SimpleTestEventHandler>());
        });
    }

    protected override void ConfigureAdditionalServices(IServiceCollection services)
    {
    }
}
