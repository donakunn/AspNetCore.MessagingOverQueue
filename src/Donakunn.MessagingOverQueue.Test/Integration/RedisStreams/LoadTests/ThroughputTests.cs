using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.TestMessages;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests;

/// <summary>
/// Throughput tests: Sustained publish rate for configurable duration.
/// Measures messages/second sustained rate and verifies no message loss.
/// </summary>
[Trait("Category", "LoadTest")]
public class ThroughputTests : LoadTestBase
{
    public ThroughputTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    [Trait("Duration", "Long")]
    public async Task Sustained_Throughput_For_10_Minutes_No_Message_Loss()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        using var host = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 100)
                   .WithCountBasedRetention(100000));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        await WarmupAsync<LoadTestEventHandler>(publisher);

        var testDuration = Config.ThroughputTestDuration;
        var targetRps = Config.ThroughputTargetMessagesPerSecond;

        Reporter.WriteLine($"Starting throughput test: {testDuration} at {targetRps} msg/sec target");

        // Act
        Metrics.Start();
        StartPeriodicReporting();

        await PublishAtRateAsync(publisher, targetRps, testDuration);

        // Wait for consumption to complete
        var expectedMessages = Metrics.GetSnapshot().TotalPublished;
        Reporter.WriteLine($"Publishing complete. Total: {expectedMessages}. Waiting for consumption...");

        await WaitForConsumptionAsync(expectedMessages, TimeSpan.FromMinutes(2));

        Metrics.Stop();

        // Assert
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Sustained Throughput Test");

        AssertNoMessageLoss();

        var minAcceptableRate = targetRps * 0.8;
        Assert.True(
            finalMetrics.PublishRatePerSecond >= minAcceptableRate,
            $"Publish rate {finalMetrics.PublishRatePerSecond:N1} should be at least 80% of target ({minAcceptableRate:N1})");
    }

    [Theory]
    [InlineData(100)]
    [InlineData(500)]
    [InlineData(1000)]
    [Trait("Duration", "Medium")]
    public async Task Throughput_At_Various_Rates(int targetRps)
    {
        // Arrange
        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        var batchSize = Math.Max(10, targetRps / 10);

        using var host = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: batchSize));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        await WarmupAsync<LoadTestEventHandler>(publisher, 50);

        Reporter.WriteLine($"Testing throughput at {targetRps} msg/sec");

        // Act
        Metrics.Start();
        var duration = TimeSpan.FromMinutes(1);
        await PublishAtRateAsync(publisher, targetRps, duration);

        var expectedMessages = Metrics.GetSnapshot().TotalPublished;
        await WaitForConsumptionAsync(expectedMessages, TimeSpan.FromMinutes(1));

        Metrics.Stop();

        // Assert
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, $"Throughput at {targetRps} msg/sec");

        AssertNoMessageLoss();
    }

    [Fact]
    [Trait("Duration", "Medium")]
    public async Task Concurrent_Publishers_Throughput()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        using var host = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 100));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        await WarmupAsync<LoadTestEventHandler>(publisher, 50);

        const int publisherCount = 5;
        const int messagesPerPublisher = 2000;
        var totalMessages = publisherCount * messagesPerPublisher;

        Reporter.WriteLine($"Testing {publisherCount} concurrent publishers, {messagesPerPublisher} messages each");

        // Act
        Metrics.Start();

        var publisherTasks = Enumerable.Range(0, publisherCount)
            .Select(publisherId => Task.Run(async () =>
            {
                for (int i = 0; i < messagesPerPublisher; i++)
                {
                    var message = new LoadTestEvent
                    {
                        Sequence = publisherId * messagesPerPublisher + i,
                        PublishedAtTicks = System.Diagnostics.Stopwatch.GetTimestamp()
                    };

                    await publisher.PublishAsync(message, TestCancellation.Token);
                    Metrics.RecordPublished();
                }
            }));

        await Task.WhenAll(publisherTasks);

        Reporter.WriteLine($"All publishers complete. Waiting for consumption...");

        await WaitForConsumptionAsync(totalMessages, TimeSpan.FromMinutes(2));

        Metrics.Stop();

        // Assert
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Concurrent Publishers Throughput");

        AssertNoMessageLoss();
        Assert.Equal(totalMessages, finalMetrics.TotalPublished);
    }

    [Fact]
    [Trait("Duration", "Medium")]
    public async Task Burst_Traffic_Handling()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        using var host = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 100));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        await WarmupAsync<LoadTestEventHandler>(publisher, 50);

        const int burstSize = 5000;
        Reporter.WriteLine($"Publishing burst of {burstSize} messages");

        // Act
        Metrics.Start();

        // Publish all messages as fast as possible (burst)
        var publishTasks = Enumerable.Range(0, burstSize)
            .Select(i =>
            {
                var message = new LoadTestEvent
                {
                    Sequence = i,
                    PublishedAtTicks = System.Diagnostics.Stopwatch.GetTimestamp()
                };
                Metrics.RecordPublished();
                return publisher.PublishAsync(message, TestCancellation.Token);
            });

        await Task.WhenAll(publishTasks);

        Reporter.WriteLine($"Burst complete. Waiting for consumption...");

        await WaitForConsumptionAsync(burstSize, TimeSpan.FromMinutes(2));

        Metrics.Stop();

        // Assert
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Burst Traffic Handling");

        AssertNoMessageLoss();
    }
}
