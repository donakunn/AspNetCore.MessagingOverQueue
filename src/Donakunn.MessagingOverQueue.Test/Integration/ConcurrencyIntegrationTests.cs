using MessagingOverQueue.src.Abstractions.Publishing;
using MessagingOverQueue.src.DependencyInjection;
using MessagingOverQueue.Test.Integration.Infrastructure;
using MessagingOverQueue.Test.Integration.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using static MessagingOverQueue.src.Topology.DependencyInjection.TopologyServiceCollectionExtensions;

namespace MessagingOverQueue.Test.Integration;

/// <summary>
/// Integration tests for concurrent message processing and thread safety.
/// </summary>
public class ConcurrencyIntegrationTests : IntegrationTestBase
{
    [Fact]
    public async Task Concurrent_Message_Processing_Is_Thread_Safe()
    {
        // Arrange
        ConcurrencyTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int messageCount = 50;

        // Act
        var publishTasks = Enumerable.Range(0, messageCount)
            .Select(i => publisher.PublishAsync(new ConcurrencyTestEvent { Index = i }));
        
        await Task.WhenAll(publishTasks);
        await ConcurrencyTestEventHandler.WaitForCountAsync(messageCount, TimeSpan.FromSeconds(60));

        // Assert
        Assert.Equal(messageCount, ConcurrencyTestEventHandler.HandleCount);
        
        // Verify all messages were processed
        var handledIndices = ConcurrencyTestEventHandler.HandledMessages
            .Select(m => m.Index)
            .OrderBy(x => x)
            .ToList();
        Assert.Equal(messageCount, handledIndices.Count);
    }

    [Fact]
    public async Task Multiple_Publishers_Can_Publish_Concurrently()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        
        const int publisherCount = 10;
        const int messagesPerPublisher = 20;
        const int totalMessages = publisherCount * messagesPerPublisher;

        // Act - Multiple publishers publishing concurrently
        var publisherTasks = Enumerable.Range(0, publisherCount)
            .Select(publisherId => Task.Run(async () =>
            {
                for (int i = 0; i < messagesPerPublisher; i++)
                {
                    await publisher.PublishAsync(new SimpleTestEvent 
                    { 
                        Value = $"Publisher-{publisherId}-Msg-{i}" 
                    });
                }
            }));

        await Task.WhenAll(publisherTasks);
        await SimpleTestEventHandler.WaitForCountAsync(totalMessages, TimeSpan.FromSeconds(120));

        // Assert
        Assert.Equal(totalMessages, SimpleTestEventHandler.HandleCount);
    }

    [Fact]
    public async Task Stress_Test_High_Message_Volume()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int messageCount = 200;

        // Act - Rapid fire publishing
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var tasks = new List<Task>(messageCount);
        
        for (int i = 0; i < messageCount; i++)
        {
            tasks.Add(publisher.PublishAsync(new SimpleTestEvent { Value = $"Stress-{i}" }));
        }

        await Task.WhenAll(tasks);
        var publishTime = sw.Elapsed;

        await SimpleTestEventHandler.WaitForCountAsync(messageCount, TimeSpan.FromSeconds(120));
        var totalTime = sw.Elapsed;

        // Assert
        Assert.Equal(messageCount, SimpleTestEventHandler.HandleCount);
        
        // Log performance metrics
        var messagesPerSecond = messageCount / totalTime.TotalSeconds;
        Assert.True(messagesPerSecond > 1, $"Should process more than 1 msg/sec, got {messagesPerSecond:F2}");
    }

    [Fact]
    public async Task Slow_Handler_Does_Not_Block_Other_Messages()
    {
        // Arrange
        SlowProcessingEventHandler.Reset();
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act - Send slow events first, then fast events
        await publisher.PublishAsync(new SlowProcessingEvent 
        { 
            Value = "Slow1", 
            ProcessingTime = TimeSpan.FromSeconds(1) 
        });
        
        // Send fast events immediately after
        for (int i = 0; i < 5; i++)
        {
            await publisher.PublishAsync(new SimpleTestEvent { Value = $"Fast-{i}" });
        }

        // Wait for fast events (should complete before slow event finishes)
        await SimpleTestEventHandler.WaitForCountAsync(5, TimeSpan.FromSeconds(10));

        // Assert fast events were processed
        Assert.Equal(5, SimpleTestEventHandler.HandleCount);
    }

    [Fact]
    public async Task Handler_Counter_Is_Thread_Safe_Under_Concurrent_Access()
    {
        // Arrange
        ConcurrencyTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int batchSize = 100;

        // Act - Fire all messages at once
        var tasks = Enumerable.Range(0, batchSize)
            .Select(i => publisher.PublishAsync(new ConcurrencyTestEvent { Index = i }))
            .ToArray();

        await Task.WhenAll(tasks);
        await ConcurrencyTestEventHandler.WaitForCountAsync(batchSize, TimeSpan.FromSeconds(60));

        // Assert - Counter should accurately reflect processed messages
        Assert.Equal(batchSize, ConcurrencyTestEventHandler.HandleCount);
        Assert.Equal(batchSize, ConcurrencyTestEventHandler.HandledMessages.Count);
    }

    [Fact]
    public async Task Concurrent_Publishing_And_Handling_Maintains_Message_Integrity()
    {
        // Arrange
        ComplexTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int messageCount = 30;

        // Create messages with unique identifiers
        var originalMessages = Enumerable.Range(0, messageCount)
            .Select(i => new ComplexTestEvent
            {
                Name = $"Message-{i}",
                Count = i,
                Amount = i * 1.5m,
                Tags = [$"tag-{i}"]
            })
            .ToList();

        // Act - Publish all concurrently
        var publishTasks = originalMessages.Select(m => publisher.PublishAsync(m));
        await Task.WhenAll(publishTasks);
        await ComplexTestEventHandler.WaitForCountAsync(messageCount, TimeSpan.FromSeconds(60));

        // Assert - Verify message content integrity
        Assert.Equal(messageCount, ComplexTestEventHandler.HandleCount);
        
        var handledNames = ComplexTestEventHandler.HandledMessages
            .Select(m => m.Name)
            .OrderBy(n => n)
            .ToList();
        
        var originalNames = originalMessages
            .Select(m => m.Name)
            .OrderBy(n => n)
            .ToList();
        
        Assert.Equal(originalNames, handledNames);
    }

    [Fact]
    public async Task Burst_Traffic_Is_Handled_Without_Message_Loss()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        
        const int burstCount = 3;
        const int messagesPerBurst = 25;
        const int totalMessages = burstCount * messagesPerBurst;

        // Act - Send messages in bursts
        for (int burst = 0; burst < burstCount; burst++)
        {
            var burstTasks = Enumerable.Range(0, messagesPerBurst)
                .Select(i => publisher.PublishAsync(new SimpleTestEvent 
                { 
                    Value = $"Burst-{burst}-Msg-{i}" 
                }));
            
            await Task.WhenAll(burstTasks);
            await Task.Delay(100); // Brief pause between bursts
        }

        await SimpleTestEventHandler.WaitForCountAsync(totalMessages, TimeSpan.FromSeconds(90));

        // Assert
        Assert.Equal(totalMessages, SimpleTestEventHandler.HandleCount);
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
                options.WithConnectionName($"ConcurrencyTests-{Guid.NewGuid():N}");
            })
            .AddTopology(topology => topology
                .WithServiceName("concurrency-test-service")
                .ScanAssemblyContaining<ConcurrencyTestEventHandler>());
        });
    }

    protected override void ConfigureAdditionalServices(IServiceCollection services)
    {
    }
}
