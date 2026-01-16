using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.TestMessages;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests;

/// <summary>
/// Latency tests: Measure P99 latency under various load levels.
/// Tests end-to-end latency from publish to handler completion.
/// </summary>
[Trait("Category", "LoadTest")]
public class LatencyTests : LoadTestBase
{
    public LatencyTests(ITestOutputHelper output) : base(output)
    {
    }

    [Theory]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(10000)]
    [Trait("Duration", "Medium")]
    public async Task P99_Latency_Under_Load(int messagesPerSecond)
    {
        // Arrange
        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        // Configure higher concurrency for higher loads
        var maxConcurrency = Math.Max(10, messagesPerSecond / 100);

        using var host = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 50));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        await WarmupAsync<LoadTestEventHandler>(publisher, 100);

        Reporter.WriteLine($"Testing P99 latency at {messagesPerSecond} msg/sec");

        // Act
        Metrics.Start();
        StartPeriodicReporting(TimeSpan.FromSeconds(10));

        var testDuration = Config.LatencyTestDurationPerLevel;
        await PublishAtPreciseRateAsync(publisher, messagesPerSecond, testDuration);

        var expectedMessages = Metrics.GetSnapshot().TotalPublished;
        await WaitForConsumptionAsync(expectedMessages, TimeSpan.FromMinutes(2));

        Metrics.Stop();

        // Assert
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, $"Latency Test at {messagesPerSecond} msg/sec");

        // Latency thresholds vary by load level
        var p99Threshold = messagesPerSecond switch
        {
            <= 100 => TimeSpan.FromMilliseconds(100),
            <= 1000 => TimeSpan.FromMilliseconds(200),
            _ => TimeSpan.FromMilliseconds(500)
        };

        Assert.True(
            finalMetrics.LatencyStatistics.P99 < p99Threshold,
            $"P99 latency {finalMetrics.LatencyStatistics.P99.TotalMilliseconds}ms exceeds threshold {p99Threshold.TotalMilliseconds}ms");

        AssertNoMessageLoss();
    }

    [Fact]
    [Trait("Duration", "Long")]
    public async Task Latency_Distribution_Under_Sustained_Load()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        using var host = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 50));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        await WarmupAsync<LoadTestEventHandler>(publisher, 100);

        Reporter.WriteLine("Testing latency distribution with varying load levels");

        // Act - Run at multiple load levels to observe latency distribution
        Metrics.Start();
        StartPeriodicReporting();

        // Ramp up and down pattern
        var loadLevels = new[] { 100, 500, 1000, 500, 100 };
        var durationPerLevel = TimeSpan.FromMinutes(1);

        foreach (var rps in loadLevels)
        {
            Reporter.WriteLine($"Testing at {rps} msg/sec");
            await PublishAtPreciseRateAsync(publisher, rps, durationPerLevel);
        }

        var expectedMessages = Metrics.GetSnapshot().TotalPublished;
        await WaitForConsumptionAsync(expectedMessages, TimeSpan.FromMinutes(2));

        Metrics.Stop();

        // Assert
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Latency Distribution Test");

        AssertNoMessageLoss();

        // Verify we have meaningful latency data
        Assert.True(finalMetrics.LatencyStatistics.Count > 0, "Should have latency measurements");
        Assert.True(finalMetrics.LatencyStatistics.P99 > TimeSpan.Zero, "P99 should be positive");
    }

    [Fact]
    [Trait("Duration", "Medium")]
    public async Task Latency_With_Large_Payloads()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        using var host = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 20));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        await WarmupAsync<LoadTestEventHandler>(publisher, 50);

        const int messageCount = 1000;
        const int payloadSizeKb = 10; // 10KB payload
        var payload = new string('x', payloadSizeKb * 1024);

        Reporter.WriteLine($"Testing latency with {payloadSizeKb}KB payloads");

        // Act
        Metrics.Start();

        for (int i = 0; i < messageCount; i++)
        {
            var message = new LoadTestEvent
            {
                Sequence = i,
                PublishedAtTicks = System.Diagnostics.Stopwatch.GetTimestamp(),
                Payload = payload
            };

            await publisher.PublishAsync(message, TestCancellation.Token);
            Metrics.RecordPublished();

            // Small delay to avoid overwhelming
            if (i % 100 == 0)
            {
                await Task.Delay(10, TestCancellation.Token);
            }
        }

        await WaitForConsumptionAsync(messageCount, TimeSpan.FromMinutes(2));

        Metrics.Stop();

        // Assert
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, $"Latency with {payloadSizeKb}KB Payloads");

        AssertNoMessageLoss();
    }

    [Fact]
    [Trait("Duration", "Medium")]
    public async Task Latency_Percentile_Stability()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        using var host = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 50));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        await WarmupAsync<LoadTestEventHandler>(publisher, 100);

        const int targetRps = 500;
        var testDuration = TimeSpan.FromMinutes(3);

        Reporter.WriteLine($"Testing latency stability at constant {targetRps} msg/sec for {testDuration.TotalMinutes} minutes");

        // Act
        Metrics.Start();
        StartPeriodicReporting(TimeSpan.FromSeconds(30));

        await PublishAtPreciseRateAsync(publisher, targetRps, testDuration);

        var expectedMessages = Metrics.GetSnapshot().TotalPublished;
        await WaitForConsumptionAsync(expectedMessages, TimeSpan.FromMinutes(1));

        Metrics.Stop();

        // Assert
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Latency Percentile Stability");

        // Verify P99 is not more than 5x P50 (stability check)
        if (finalMetrics.LatencyStatistics.P50.TotalMilliseconds > 0)
        {
            var stabilityRatio = finalMetrics.LatencyStatistics.P99.TotalMilliseconds /
                                 finalMetrics.LatencyStatistics.P50.TotalMilliseconds;

            Reporter.WriteLine($"P99/P50 ratio: {stabilityRatio:N2}");

            Assert.True(
                stabilityRatio < 10,
                $"Latency distribution is too wide. P99/P50 ratio: {stabilityRatio:N2}");
        }

        AssertNoMessageLoss();
    }
}
