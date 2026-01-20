using System.Diagnostics;
using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.Persistence;
using Donakunn.MessagingOverQueue.Persistence.Entities;
using Donakunn.MessagingOverQueue.Persistence.Providers;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.TestMessages;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests;

/// <summary>
/// Load tests for persistence features (outbox and idempotency) under load.
/// Tests database performance, recovery, and duplicate handling.
/// </summary>
[Trait("Category", "LoadTest")]
[Trait("Category", "Persistence")]
public class PersistenceUnderLoadTests : LoadTestBase
{
    public PersistenceUnderLoadTests(ITestOutputHelper output) : base(output)
    {
    }

    /// <summary>
    /// Tests outbox throughput at various batch sizes.
    /// </summary>
    [Theory]
    [InlineData(10)]
    [InlineData(50)]
    [InlineData(100)]
    [InlineData(200)]
    public async Task Outbox_Throughput_At_Various_Batch_Sizes(int batchSize)
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(
            outbox: true,
            outboxBatchSize: batchSize,
            outboxProcessingInterval: TimeSpan.FromMilliseconds(100));

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing outbox throughput with batch size {batchSize}");

        LoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act
        var targetRps = 200;
        var duration = TimeSpan.FromMinutes(1);
        await PublishAtRateAsync(publisher, targetRps, duration);

        var snapshot = Metrics.GetSnapshot();
        await WaitForConsumptionAsync(snapshot.TotalPublished, TimeSpan.FromMinutes(3));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.WriteLine($"Batch Size: {batchSize}");
        Reporter.WriteLine($"  Published: {finalMetrics.TotalPublished}");
        Reporter.WriteLine($"  Consumed: {finalMetrics.TotalConsumed}");
        Reporter.WriteLine($"  Throughput: {finalMetrics.ConsumeRatePerSecond:F2} msg/s");
        Reporter.WriteLine($"  P99 Latency: {finalMetrics.LatencyStatistics.P99.TotalMilliseconds:F2}ms");

        // Assert
        AssertNoMessageLoss(2.0); // Outbox may have slight delays
    }

    /// <summary>
    /// Tests outbox recovery after simulated publisher crash.
    /// Messages stored in outbox but not yet published should be recovered.
    /// NOTE: This test is skipped because it requires deeper investigation into
    /// SQL Server outbox processor recovery across host restarts.
    /// </summary>
    [Fact]
    public async Task Outbox_Recovery_After_Publisher_Crash()
    {
        // Arrange
        var features = CreateFeatures(
            outbox: true,
            outboxBatchSize: 50,
            outboxProcessingInterval: TimeSpan.FromSeconds(5)); // Slow interval to simulate crash before publish

        await EnsureSqlServerAsync();
        Reporter.WriteLine($"Testing outbox recovery after crash");

        // Phase 1: Start publisher, add messages to outbox, then "crash" (stop host)
        LoadTestEventHandler.Reset();
        var host1 = await BuildHostWithFeatures<LoadTestEventHandler>(features);

        var messagesBeforeCrash = 100;
        using (var scope = host1.Services.CreateScope())
        {
            var outboxPublisher = scope.ServiceProvider.GetRequiredService<OutboxPublisher>();

            for (int i = 0; i < messagesBeforeCrash; i++)
            {
                var message = new LoadTestEvent
                {
                    Sequence = i,
                    PublishedAtTicks = Stopwatch.GetTimestamp()
                };
                await ((IEventPublisher)outboxPublisher).PublishAsync(message);
            }
        }

        Reporter.WriteLine($"Published {messagesBeforeCrash} messages to outbox");

        // Check outbox has pending messages
        using (var scope = host1.Services.CreateScope())
        {
            var storeProvider = scope.ServiceProvider.GetRequiredService<IMessageStoreProvider>();
            var pending = await storeProvider.AcquireOutboxLockAsync(1000, TimeSpan.FromMinutes(1));
            Reporter.WriteLine($"Pending in outbox before crash: {pending.Count}");

            // Release the lock
            foreach (var entry in pending)
            {
                await storeProvider.ReleaseLockAsync(entry.Id);
            }
        }

        // "Crash" the host - stop without waiting for outbox to flush
        using var stopCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        try { await host1.StopAsync(stopCts.Token); } catch (OperationCanceledException) { }
        host1.Dispose();

        Reporter.WriteLine("Host crashed (stopped abruptly)");

        // Phase 2: Start new host with faster outbox processing
        var features2 = CreateFeatures(
            outbox: true,
            outboxBatchSize: 100,
            outboxProcessingInterval: TimeSpan.FromMilliseconds(100),
            idempotency: true,
            retry:  true,
            retryMaxAttempts: 10,
            retryInitialDelay: TimeSpan.FromSeconds(3),
            circuitBreaker: true);

        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        using var host2 = await BuildHostWithFeatures<LoadTestEventHandler>(features2);

        // Wait for outbox processor to recover and publish messages
        await Task.Delay(TimeSpan.FromSeconds(30));

        // Report
        Reporter.WriteLine($"After recovery:");
        Reporter.WriteLine($"  Handler count: {LoadTestEventHandler.HandleCount}");

        // Assert - Messages should be recovered from outbox
        Assert.True(LoadTestEventHandler.HandleCount >= messagesBeforeCrash * 0.9,
            $"Expected at least 90% of messages ({messagesBeforeCrash * 0.9}) to be recovered");
    }

    /// <summary>
    /// Tests idempotency prevents duplicates under high load.
    /// </summary>
    [Fact]
    public async Task Idempotency_Prevents_Duplicates_Under_Load()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(idempotency: true);

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing idempotency duplicate prevention");

        LoadTestEventHandler.SetMetricsCollector(Metrics);

        // Act - Publish same message multiple times
        var uniqueMessages = 100;
        var duplicatesPerMessage = 5;

        for (int i = 0; i < uniqueMessages; i++)
        {
            var message = new LoadTestEvent
            {
                Sequence = i,
                PublishedAtTicks = Stopwatch.GetTimestamp()
            };

            // Publish same message multiple times
            for (int dup = 0; dup < duplicatesPerMessage; dup++)
            {
                await publisher.PublishAsync(message, TestCancellation.Token);
                Metrics.RecordPublished();
            }
        }

        // Wait for processing
        await Task.Delay(TimeSpan.FromSeconds(30));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.WriteLine($"Unique messages: {uniqueMessages}");
        Reporter.WriteLine($"Total published (with duplicates): {finalMetrics.TotalPublished}");
        Reporter.WriteLine($"Handler invocations: {LoadTestEventHandler.HandleCount}");

        // Assert - Handler should only be called once per unique message
        Assert.Equal(uniqueMessages, LoadTestEventHandler.HandleCount);
    }

    /// <summary>
    /// Tests idempotency database performance at scale.
    /// </summary>
    [Fact]
    public async Task Idempotency_Database_Performance_At_Scale()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(idempotency: true);

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing idempotency at scale");

        LoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act - Publish large number of unique messages
        var targetRps = 300;
        var duration = TimeSpan.FromMinutes(2);
        await PublishAtRateAsync(publisher, targetRps, duration);

        var snapshot = Metrics.GetSnapshot();
        await WaitForConsumptionAsync(snapshot.TotalPublished, TimeSpan.FromMinutes(5));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Test");
        Reporter.WriteLine($"Total messages processed: {finalMetrics.TotalConsumed}");
        Reporter.WriteLine($"Database operations: ~{finalMetrics.TotalConsumed * 2} (check + insert)");

        // Assert
        AssertNoMessageLoss(0.5);

        // Check latency is reasonable despite database operations
        Assert.True(finalMetrics.LatencyStatistics.P99 < TimeSpan.FromSeconds(5),
            $"P99 latency {finalMetrics.LatencyStatistics.P99.TotalMilliseconds}ms exceeds 5s threshold");
    }

    /// <summary>
    /// Tests outbox plus idempotency for end-to-end exactly-once semantics.
    /// </summary>
    [Fact]
    public async Task Outbox_Plus_Idempotency_End_To_End()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(
            outbox: true,
            outboxBatchSize: 50,
            outboxProcessingInterval: TimeSpan.FromMilliseconds(100),
            idempotency: true);

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing exactly-once semantics with outbox + idempotency");

        LoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act
        var targetRps = 100;
        var duration = TimeSpan.FromMinutes(1);
        await PublishAtRateAsync(publisher, targetRps, duration);

        var snapshot = Metrics.GetSnapshot();
        await WaitForConsumptionAsync(snapshot.TotalPublished, TimeSpan.FromMinutes(3));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Test");

        // Assert - Exactly-once: published == consumed == handled
        Assert.Equal(finalMetrics.TotalPublished, finalMetrics.TotalConsumed);
        Assert.Equal(finalMetrics.TotalPublished, LoadTestEventHandler.HandleCount);
    }

    /// <summary>
    /// Tests SQL Server connection pool behavior under high load.
    /// </summary>
    [Fact]
    public async Task SQL_Server_Connection_Pool_Under_Load()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(
            outbox: true,
            outboxBatchSize: 20,
            outboxProcessingInterval: TimeSpan.FromMilliseconds(50),
            idempotency: true);

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing SQL Server connection pool under load");

        LoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act - High concurrent load to stress connection pool
        var concurrentPublishers = 10;
        var messagesPerPublisher = 500;
        var publishTasks = new List<Task>();

        for (int p = 0; p < concurrentPublishers; p++)
        {
            var publisherId = p;
            publishTasks.Add(Task.Run(async () =>
            {
                for (int i = 0; i < messagesPerPublisher; i++)
                {
                    var message = new LoadTestEvent
                    {
                        Sequence = publisherId * messagesPerPublisher + i,
                        PublishedAtTicks = Stopwatch.GetTimestamp()
                    };
                    await publisher.PublishAsync(message, TestCancellation.Token);
                    Metrics.RecordPublished();

                    // Small delay to spread load
                    await Task.Delay(5, TestCancellation.Token);
                }
            }));
        }

        await Task.WhenAll(publishTasks);

        var totalExpected = concurrentPublishers * messagesPerPublisher;
        await WaitForConsumptionAsync(totalExpected, TimeSpan.FromMinutes(5));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Test");
        Reporter.WriteLine($"Concurrent publishers: {concurrentPublishers}");
        Reporter.WriteLine($"Messages per publisher: {messagesPerPublisher}");
        Reporter.WriteLine($"Total expected: {totalExpected}");

        // Assert
        AssertNoMessageLoss(1.0);
    }
}
