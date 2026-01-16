using System.Diagnostics;
using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.TestMessages;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests;

/// <summary>
/// Stress tests: Consumer slower than publisher for extended periods.
/// Tests backpressure handling and eventual consistency.
/// </summary>
[Trait("Category", "LoadTest")]
[Trait("Duration", "VeryLong")]
public class StressTests : LoadTestBase
{
    public StressTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    [Trait("Duration", "VeryLong")]
    public async Task Consumer_Slower_Than_Publisher_For_1_Hour()
    {
        // Arrange
        SlowLoadTestEventHandler.Reset();
        SlowLoadTestEventHandler.SetMetricsCollector(Metrics);

        // Configure slow consumer (50ms per message with low concurrency = ~200 msg/sec max)
        using var host = await BuildHost<SlowLoadTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 10)
                   .WithCountBasedRetention(1000000)); // Large retention for stress test

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Warmup with slow handler
        Reporter.WriteLine("Warming up slow handler...");
        for (int i = 0; i < 20; i++)
        {
            await publisher.PublishAsync(new SlowLoadTestEvent { Sequence = i }, TestCancellation.Token);
        }
        await SlowLoadTestEventHandler.WaitForCountAsync(20, TimeSpan.FromSeconds(30));
        SlowLoadTestEventHandler.Reset();
        Metrics.Reset();

        // Act
        var testDuration = Config.StressTestDuration;
        var publishRate = Config.StressTestPublisherRatePerSecond;

        Reporter.WriteLine($"Starting stress test: {testDuration} at {publishRate} msg/sec publish rate");
        Reporter.WriteLine($"Consumer processes at ~{1000 / 50 * 10} msg/sec (50ms delay, 10 concurrency)");

        Metrics.Start();
        StartPeriodicReporting(TimeSpan.FromMinutes(1));

        // Publisher task - continuous publishing at configured rate
        var stopwatch = Stopwatch.StartNew();
        long sequence = 0;
        var batchSize = 10;
        var batchDelay = TimeSpan.FromMilliseconds(1000.0 / publishRate * batchSize);

        while (stopwatch.Elapsed < testDuration && !TestCancellation.IsCancellationRequested)
        {
            var tasks = new List<Task>(batchSize);
            for (int i = 0; i < batchSize; i++)
            {
                var msg = new SlowLoadTestEvent
                {
                    Sequence = Interlocked.Increment(ref sequence),
                    PublishedAtTicks = Stopwatch.GetTimestamp(),
                    ProcessingDelay = TimeSpan.FromMilliseconds(50)
                };
                tasks.Add(publisher.PublishAsync(msg, TestCancellation.Token));
                Metrics.RecordPublished();
            }
            await Task.WhenAll(tasks);
            await Task.Delay(batchDelay, TestCancellation.Token);
        }

        // Allow time for backlog to drain
        Reporter.WriteLine("Publishing complete. Waiting for backlog to drain...");

        var drainTimeout = Config.StressTestDrainTimeout;
        var expectedMessages = Metrics.GetSnapshot().TotalPublished;
        await WaitForSlowConsumptionAsync(expectedMessages, drainTimeout);

        Metrics.Stop();

        // Assert
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Stress Test (Consumer Slower Than Publisher)");

        // Verify eventual consistency
        Assert.Equal(finalMetrics.TotalPublished, finalMetrics.TotalConsumed);
        AssertNoMessageLoss();
    }

    [Fact]
    [Trait("Duration", "Long")]
    public async Task Backpressure_Handled_Gracefully()
    {
        // Arrange
        SlowLoadTestEventHandler.Reset();
        SlowLoadTestEventHandler.SetMetricsCollector(Metrics);

        using var host = await BuildHost<SlowLoadTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 5));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act - Rapid burst publish
        const int burstSize = 5000;
        Metrics.Start();

        Reporter.WriteLine($"Publishing {burstSize} messages in burst to slow consumer...");

        var publishTasks = Enumerable.Range(0, burstSize)
            .Select(i =>
            {
                var msg = new SlowLoadTestEvent
                {
                    Sequence = i,
                    ProcessingDelay = TimeSpan.FromMilliseconds(20)
                };
                Metrics.RecordPublished();
                return publisher.PublishAsync(msg, TestCancellation.Token);
            });

        await Task.WhenAll(publishTasks);

        Reporter.WriteLine("Burst complete. Monitoring consumption...");
        StartPeriodicReporting(TimeSpan.FromSeconds(10));

        // Wait for complete consumption
        await WaitForSlowConsumptionAsync(burstSize, TimeSpan.FromMinutes(15));

        Metrics.Stop();

        // Assert
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Backpressure Handling Test");

        AssertNoMessageLoss();
    }

    [Fact]
    [Trait("Duration", "Long")]
    public async Task Memory_Stability_Under_Sustained_Load()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        using var host = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 50));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        await WarmupAsync<LoadTestEventHandler>(publisher, 100);

        // Act - Monitor memory over extended period
        var memorySnapshots = new List<(TimeSpan Elapsed, long MemoryMB)>();
        var initialMemory = GC.GetTotalMemory(true) / 1024 / 1024;

        Metrics.Start();
        var duration = TimeSpan.FromMinutes(10);
        var stopwatch = Stopwatch.StartNew();
        long sequence = 0;
        var lastMemorySample = TimeSpan.Zero;

        Reporter.WriteLine($"Testing memory stability over {duration.TotalMinutes} minutes");
        Reporter.WriteLine($"Initial Memory: {initialMemory}MB");

        while (stopwatch.Elapsed < duration && !TestCancellation.IsCancellationRequested)
        {
            // Publish batch with payload
            var tasks = Enumerable.Range(0, 100)
                .Select(_ => publisher.PublishAsync(new LoadTestEvent
                {
                    Sequence = Interlocked.Increment(ref sequence),
                    Payload = new string('x', 1000) // 1KB payload
                }, TestCancellation.Token));

            await Task.WhenAll(tasks);

            for (int i = 0; i < 100; i++)
                Metrics.RecordPublished();

            // Sample memory every minute
            if (stopwatch.Elapsed - lastMemorySample > TimeSpan.FromMinutes(1))
            {
                var currentMemory = GC.GetTotalMemory(false) / 1024 / 1024;
                memorySnapshots.Add((stopwatch.Elapsed, currentMemory));
                Reporter.WriteLine($"[{stopwatch.Elapsed:mm\\:ss}] Memory: {currentMemory}MB, Messages: {sequence}");
                lastMemorySample = stopwatch.Elapsed;
            }

            await Task.Delay(100, TestCancellation.Token);
        }

        await WaitForConsumptionAsync(sequence, TimeSpan.FromMinutes(5));
        Metrics.Stop();

        // Force GC and measure final memory
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var finalMemory = GC.GetTotalMemory(true) / 1024 / 1024;
        var memoryGrowth = finalMemory - initialMemory;

        Reporter.WriteLine($"Final Memory: {finalMemory}MB");
        Reporter.WriteLine($"Memory Growth: {memoryGrowth}MB");

        // Assert
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Memory Stability Test");

        // Memory should not grow unbounded (allow some growth for test infrastructure)
        Assert.True(
            memoryGrowth < 500,
            $"Memory grew by {memoryGrowth}MB, expected < 500MB growth");

        AssertNoMessageLoss();
    }

    [Fact]
    [Trait("Duration", "Long")]
    public async Task Sustained_High_Volume_No_Resource_Exhaustion()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        using var host = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 100)
                   .WithCountBasedRetention(500000));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        await WarmupAsync<LoadTestEventHandler>(publisher, 100);

        const int targetRps = 2000;
        var duration = TimeSpan.FromMinutes(5);

        Reporter.WriteLine($"High volume test: {targetRps} msg/sec for {duration.TotalMinutes} minutes");

        // Act
        Metrics.Start();
        StartPeriodicReporting(TimeSpan.FromSeconds(30));

        await PublishAtRateAsync(publisher, targetRps, duration);

        var expectedMessages = Metrics.GetSnapshot().TotalPublished;
        await WaitForConsumptionAsync(expectedMessages, TimeSpan.FromMinutes(3));

        Metrics.Stop();

        // Assert
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "High Volume Stress Test");

        AssertNoMessageLoss();

        // Should have published close to target
        var expectedTotal = targetRps * duration.TotalSeconds;
        Assert.True(
            finalMetrics.TotalPublished >= expectedTotal * 0.8,
            $"Published {finalMetrics.TotalPublished}, expected at least {expectedTotal * 0.8:N0}");
    }

    private async Task WaitForSlowConsumptionAsync(long expectedCount, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (SlowLoadTestEventHandler.HandleCount < expectedCount && sw.Elapsed < timeout)
        {
            if (TestCancellation.IsCancellationRequested)
                break;

            await Task.Delay(500, TestCancellation.Token);
        }
    }
}
