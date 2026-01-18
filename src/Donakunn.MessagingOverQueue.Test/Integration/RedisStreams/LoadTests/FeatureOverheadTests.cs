using System.Diagnostics;
using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.TestMessages;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests;

/// <summary>
/// Load tests measuring the performance overhead of individual middleware features.
/// These tests compare throughput and latency with and without specific features enabled.
/// </summary>
[Trait("Category", "LoadTest")]
[Trait("Category", "FeatureOverhead")]
public class FeatureOverheadTests : LoadTestBase
{
    public FeatureOverheadTests(ITestOutputHelper output) : base(output)
    {
    }

    /// <summary>
    /// Baseline performance test with no features enabled.
    /// Provides reference metrics for comparison with feature-enabled tests.
    /// </summary>
    [Fact]
    public async Task Baseline_Performance_No_Features()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = FeatureConfiguration.None;

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing baseline performance (no features)");
        Reporter.WriteLine($"Target RPS: {Config.ThroughputTargetMessagesPerSecond}");

        // Warmup
        await WarmupAsync<LoadTestEventHandler>(publisher);

        LoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act
        var duration = TimeSpan.FromMinutes(2);
        await PublishAtRateAsync(publisher, Config.ThroughputTargetMessagesPerSecond, duration);

        var snapshot = Metrics.GetSnapshot();
        await WaitForConsumptionAsync(snapshot.TotalPublished, TimeSpan.FromMinutes(1));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Baseline Performance");

        // Assert
        AssertNoMessageLoss(0.1); // Allow 0.1% loss
        Reporter.WriteLine($"Baseline P50: {finalMetrics.LatencyStatistics.P50.TotalMilliseconds:F2}ms");
        Reporter.WriteLine($"Baseline P99: {finalMetrics.LatencyStatistics.P99.TotalMilliseconds:F2}ms");
    }

    /// <summary>
    /// Measures retry middleware overhead when no failures occur.
    /// </summary>
    [Fact]
    public async Task Retry_Overhead_Under_Load()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(retry: true, retryMaxAttempts: 3);

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing retry middleware overhead");
        Reporter.WriteLine($"Configuration: MaxAttempts={features.RetryMaxAttempts}");

        await WarmupAsync<LoadTestEventHandler>(publisher);

        LoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act
        var duration = TimeSpan.FromMinutes(2);
        await PublishAtRateAsync(publisher, Config.ThroughputTargetMessagesPerSecond, duration);

        var snapshot = Metrics.GetSnapshot();
        await WaitForConsumptionAsync(snapshot.TotalPublished, TimeSpan.FromMinutes(1));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Test");

        // Assert
        AssertNoMessageLoss(0.1);
        Reporter.WriteLine($"Retry middleware P50: {finalMetrics.LatencyStatistics.P50.TotalMilliseconds:F2}ms");
        Reporter.WriteLine($"Retry middleware P99: {finalMetrics.LatencyStatistics.P99.TotalMilliseconds:F2}ms");
    }

    /// <summary>
    /// Measures circuit breaker overhead when circuit is healthy (closed).
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_Overhead_Under_Load()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(circuitBreaker: true, circuitBreakerThreshold: 10);

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing circuit breaker overhead (healthy state)");
        Reporter.WriteLine($"Configuration: FailureThreshold={features.CircuitBreakerFailureThreshold}");

        await WarmupAsync<LoadTestEventHandler>(publisher);

        LoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act
        var duration = TimeSpan.FromMinutes(2);
        await PublishAtRateAsync(publisher, Config.ThroughputTargetMessagesPerSecond, duration);

        var snapshot = Metrics.GetSnapshot();
        await WaitForConsumptionAsync(snapshot.TotalPublished, TimeSpan.FromMinutes(1));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Test");

        // Assert
        AssertNoMessageLoss(0.1);
        Reporter.WriteLine($"Circuit breaker P50: {finalMetrics.LatencyStatistics.P50.TotalMilliseconds:F2}ms");
        Reporter.WriteLine($"Circuit breaker P99: {finalMetrics.LatencyStatistics.P99.TotalMilliseconds:F2}ms");
    }

    /// <summary>
    /// Measures timeout middleware overhead when no timeouts occur.
    /// </summary>
    [Fact]
    public async Task Timeout_Overhead_Under_Load()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(timeout: true, timeoutDuration: TimeSpan.FromSeconds(30));

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing timeout middleware overhead");
        Reporter.WriteLine($"Configuration: Timeout={features.TimeoutDuration.TotalSeconds}s");

        await WarmupAsync<LoadTestEventHandler>(publisher);

        LoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act
        var duration = TimeSpan.FromMinutes(2);
        await PublishAtRateAsync(publisher, Config.ThroughputTargetMessagesPerSecond, duration);

        var snapshot = Metrics.GetSnapshot();
        await WaitForConsumptionAsync(snapshot.TotalPublished, TimeSpan.FromMinutes(1));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Test");

        // Assert
        AssertNoMessageLoss(0.1);
        Reporter.WriteLine($"Timeout middleware P50: {finalMetrics.LatencyStatistics.P50.TotalMilliseconds:F2}ms");
        Reporter.WriteLine($"Timeout middleware P99: {finalMetrics.LatencyStatistics.P99.TotalMilliseconds:F2}ms");
    }

    /// <summary>
    /// Measures idempotency middleware overhead (database lookup per message).
    /// </summary>
    [Fact]
    public async Task Idempotency_Overhead_Under_Load()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(idempotency: true);

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing idempotency middleware overhead");
        Reporter.WriteLine($"Configuration: RetentionPeriod={features.IdempotencyRetentionPeriod.TotalDays} days");

        await WarmupAsync<LoadTestEventHandler>(publisher);

        LoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act - Use lower RPS due to database overhead
        var targetRps = Math.Min(500, Config.ThroughputTargetMessagesPerSecond);
        var duration = TimeSpan.FromMinutes(2);
        await PublishAtRateAsync(publisher, targetRps, duration);

        var snapshot = Metrics.GetSnapshot();
        await WaitForConsumptionAsync(snapshot.TotalPublished, TimeSpan.FromMinutes(2));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Test");

        // Assert
        AssertNoMessageLoss(0.1);
        Reporter.WriteLine($"Idempotency P50: {finalMetrics.LatencyStatistics.P50.TotalMilliseconds:F2}ms");
        Reporter.WriteLine($"Idempotency P99: {finalMetrics.LatencyStatistics.P99.TotalMilliseconds:F2}ms");
    }

    /// <summary>
    /// Measures outbox pattern overhead (two-phase commit).
    /// </summary>
    [Fact]
    public async Task Outbox_Overhead_Under_Load()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(
            outbox: true,
            outboxBatchSize: 100,
            outboxProcessingInterval: TimeSpan.FromMilliseconds(100));

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing outbox pattern overhead");
        Reporter.WriteLine($"Configuration: BatchSize={features.OutboxBatchSize}, Interval={features.OutboxProcessingInterval.TotalMilliseconds}ms");

        await WarmupAsync<LoadTestEventHandler>(publisher);

        LoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act - Use lower RPS due to database overhead
        var targetRps = Math.Min(500, Config.ThroughputTargetMessagesPerSecond);
        var duration = TimeSpan.FromMinutes(2);
        await PublishAtRateAsync(publisher, targetRps, duration);

        // Wait longer for outbox processing
        var snapshot = Metrics.GetSnapshot();
        await WaitForConsumptionAsync(snapshot.TotalPublished, TimeSpan.FromMinutes(3));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Test");

        // Assert - Outbox introduces latency, so be more lenient
        AssertNoMessageLoss(1.0); // Allow 1% due to outbox delays
        Reporter.WriteLine($"Outbox P50: {finalMetrics.LatencyStatistics.P50.TotalMilliseconds:F2}ms");
        Reporter.WriteLine($"Outbox P99: {finalMetrics.LatencyStatistics.P99.TotalMilliseconds:F2}ms");
    }

    /// <summary>
    /// Measures cumulative overhead with all features enabled.
    /// </summary>
    [Fact]
    public async Task All_Features_Combined_Overhead()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(
            outbox: true,
            outboxBatchSize: 100,
            idempotency: true,
            retry: true,
            retryMaxAttempts: 3,
            circuitBreaker: true,
            circuitBreakerThreshold: 10,
            timeout: true,
            timeoutDuration: TimeSpan.FromSeconds(30));

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing all features combined overhead");
        Reporter.WriteLine($"Features: {features}");

        await WarmupAsync<LoadTestEventHandler>(publisher);

        LoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act - Lower RPS due to combined overhead
        var targetRps = Math.Min(300, Config.ThroughputTargetMessagesPerSecond);
        var duration = TimeSpan.FromMinutes(2);
        await PublishAtRateAsync(publisher, targetRps, duration);

        var snapshot = Metrics.GetSnapshot();
        await WaitForConsumptionAsync(snapshot.TotalPublished, TimeSpan.FromMinutes(5));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Test");

        // Assert
        AssertNoMessageLoss(1.0);
        Reporter.WriteLine($"All features P50: {finalMetrics.LatencyStatistics.P50.TotalMilliseconds:F2}ms");
        Reporter.WriteLine($"All features P99: {finalMetrics.LatencyStatistics.P99.TotalMilliseconds:F2}ms");
    }

    /// <summary>
    /// Compares latency impact of idempotency at various message rates.
    /// </summary>
    [Theory]
    [InlineData(100)]
    [InlineData(250)]
    [InlineData(500)]
    public async Task Latency_Impact_Of_Idempotency_At_Various_Rates(int targetRps)
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(idempotency: true);

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing idempotency latency at {targetRps} RPS");

        await WarmupAsync<LoadTestEventHandler>(publisher);

        LoadTestEventHandler.SetMetricsCollector(Metrics);

        // Act
        var duration = TimeSpan.FromMinutes(1);
        await PublishAtRateAsync(publisher, targetRps, duration);

        var snapshot = Metrics.GetSnapshot();
        await WaitForConsumptionAsync(snapshot.TotalPublished, TimeSpan.FromMinutes(2));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.WriteLine($"Rate: {targetRps} RPS");
        Reporter.WriteLine($"  P50: {finalMetrics.LatencyStatistics.P50.TotalMilliseconds:F2}ms");
        Reporter.WriteLine($"  P99: {finalMetrics.LatencyStatistics.P99.TotalMilliseconds:F2}ms");
        Reporter.WriteLine($"  Throughput: {finalMetrics.ConsumeRatePerSecond:F2} msg/s");

        // Assert
        AssertNoMessageLoss(0.5);
    }
}
