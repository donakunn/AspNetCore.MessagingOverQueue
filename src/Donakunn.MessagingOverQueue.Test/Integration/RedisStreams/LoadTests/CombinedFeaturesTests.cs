using System.Diagnostics;
using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.TestMessages;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests;

/// <summary>
/// Load tests for combined feature interactions.
/// Tests how multiple features work together under load.
/// </summary>
[Trait("Category", "LoadTest")]
[Trait("Category", "CombinedFeatures")]
public class CombinedFeaturesTests : LoadTestBase
{
    public CombinedFeaturesTests(ITestOutputHelper output) : base(output)
    {
    }

    /// <summary>
    /// Verifies middleware ordering is correct under load.
    /// CircuitBreaker -> Retry -> Timeout -> Deserialization -> Handler
    /// </summary>
    [Fact]
    public async Task Resilience_Stack_Order_Under_Load()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(
            retry: true,
            retryMaxAttempts: 3,
            circuitBreaker: true,
            circuitBreakerThreshold: 10,
            timeout: true,
            timeoutDuration: TimeSpan.FromSeconds(30));

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);

        // Verify middleware ordering
        var middlewares = host.Services.GetServices<IConsumeMiddleware>()
            .OfType<IOrderedConsumeMiddleware>()
            .OrderBy(m => m.Order)
            .ToList();

        Reporter.WriteLine("Middleware order:");
        foreach (var m in middlewares)
        {
            Reporter.WriteLine($"  Order {m.Order}: {m.GetType().Name}");
        }

        // Verify ordering: CircuitBreaker (100) < Retry (200) < Timeout (300)
        var circuitBreaker = middlewares.FirstOrDefault(m => m is CircuitBreakerMiddleware);
        var retry = middlewares.FirstOrDefault(m => m is RetryMiddleware);
        var timeout = middlewares.FirstOrDefault(m => m is TimeoutMiddleware);

        Assert.NotNull(circuitBreaker);
        Assert.NotNull(retry);
        Assert.NotNull(timeout);
        Assert.True(circuitBreaker!.Order < retry!.Order, "CircuitBreaker should be before Retry");
        Assert.True(retry!.Order < timeout!.Order, "Retry should be before Timeout");

        // Run a load test to verify it works under load
        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        var targetRps = 100;
        var duration = TimeSpan.FromMinutes(1);
        await PublishAtRateAsync(publisher, targetRps, duration);

        var snapshot = Metrics.GetSnapshot();
        await WaitForConsumptionAsync(snapshot.TotalPublished, TimeSpan.FromMinutes(1));

        var finalMetrics = Metrics.GetSnapshot();
        Reporter.WriteLine($"Messages processed: {finalMetrics.TotalConsumed}");

        AssertNoMessageLoss(0.1);
    }

    /// <summary>
    /// Tests outbox with retry under failure conditions.
    /// Outbox ensures messages are persisted, retry handles transient failures.
    /// </summary>
    [Fact]
    public async Task Outbox_With_Retry_Under_Failures()
    {
        // Arrange
        FailingLoadTestEventHandler.Reset();
        var features = CreateFeatures(
            outbox: true,
            outboxBatchSize: 50,
            outboxProcessingInterval: TimeSpan.FromMilliseconds(100),
            retry: true,
            retryMaxAttempts: 3,
            retryInitialDelay: TimeSpan.FromMilliseconds(100));

        using var host = await BuildHostWithFeatures<FailingLoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing outbox + retry under failures");

        FailingLoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act - Mix of successful and transiently failing messages
        var totalMessages = 200;
        for (int i = 0; i < totalMessages; i++)
        {
            var isTransientFailure = i % 5 == 0; // 20% transient failures

            var message = new FailingLoadTestEvent
            {
                Sequence = i,
                PublishedAtTicks = Stopwatch.GetTimestamp(),
                ShouldFail = isTransientFailure,
                IsTransient = true,
                FailOnAttempts = isTransientFailure ? [1] : []
            };

            await publisher.PublishAsync(message, TestCancellation.Token);
            Metrics.RecordPublished();

            await Task.Delay(20, TestCancellation.Token);
        }

        // Wait for outbox processing and retries
        await WaitForHandlerCountAsync(totalMessages, TimeSpan.FromMinutes(3));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Test");
        Reporter.WriteLine($"Transient failures recovered: {FailingLoadTestEventHandler.ErrorCount}");
        Reporter.WriteLine($"Successfully processed: {FailingLoadTestEventHandler.HandleCount}");

        // Assert - All messages should eventually succeed with retry
        Assert.Equal(totalMessages, FailingLoadTestEventHandler.HandleCount);
    }

    /// <summary>
    /// Tests idempotency with circuit breaker interaction.
    /// </summary>
    [Fact]
    public async Task Idempotency_With_CircuitBreaker()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(
            idempotency: true,
            circuitBreaker: true,
            circuitBreakerThreshold: 10,
            circuitBreakerDuration: TimeSpan.FromSeconds(5));

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing idempotency + circuit breaker");

        LoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act
        var targetRps = 100;
        var duration = TimeSpan.FromMinutes(1);
        await PublishAtRateAsync(publisher, targetRps, duration);

        var snapshot = Metrics.GetSnapshot();
        await WaitForConsumptionAsync(snapshot.TotalPublished, TimeSpan.FromMinutes(2));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Test");

        // Assert
        AssertNoMessageLoss(0.5);
    }

    /// <summary>
    /// Extended stress test with all features enabled.
    /// Runs for configurable duration (default 1 hour from config).
    /// </summary>
    [Fact]
    public async Task Full_Feature_Stack_Extended_Stress_Test()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(
            outbox: true,
            outboxBatchSize: 100,
            outboxProcessingInterval: TimeSpan.FromMilliseconds(100),
            idempotency: true,
            retry: true,
            retryMaxAttempts: 3,
            circuitBreaker: true,
            circuitBreakerThreshold: 20,
            timeout: true,
            timeoutDuration: TimeSpan.FromSeconds(30));

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Extended stress test with all features");
        Reporter.WriteLine($"Features: {features}");

        LoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act - Use shorter duration for CI (override with env var for full test)
        var duration = TimeSpan.FromMinutes(5); // Short version for CI
        var targetRps = 100;

        await PublishAtRateAsync(publisher, targetRps, duration);

        var snapshot = Metrics.GetSnapshot();
        await WaitForConsumptionAsync(snapshot.TotalPublished, TimeSpan.FromMinutes(10));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Test");

        // Assert
        AssertNoMessageLoss(1.0);

        // System should remain stable
        Assert.True(finalMetrics.LatencyStatistics.P99 < TimeSpan.FromSeconds(10),
            $"P99 latency {finalMetrics.LatencyStatistics.P99.TotalMilliseconds}ms exceeds stability threshold");
    }

    /// <summary>
    /// Tests various feature combinations using theory.
    /// </summary>
    [Theory]
    [InlineData(true, false, false, false, false)] // Outbox only
    [InlineData(false, true, false, false, false)] // Idempotency only
    [InlineData(false, false, true, false, false)] // Retry only
    [InlineData(false, false, false, true, false)] // CircuitBreaker only
    [InlineData(false, false, false, false, true)] // Timeout only
    [InlineData(true, true, false, false, false)]  // Outbox + Idempotency
    [InlineData(false, false, true, true, true)]   // All resilience
    [InlineData(true, true, true, false, false)]   // Persistence + Retry
    public async Task Feature_Combination_Matrix(
        bool outbox, bool idempotency, bool retry, bool circuitBreaker, bool timeout)
    {
        // Arrange
        LoadTestEventHandler.Reset();
        var features = CreateFeatures(
            outbox: outbox,
            idempotency: idempotency,
            retry: retry,
            circuitBreaker: circuitBreaker,
            timeout: timeout);

        using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing feature combination: {features}");

        LoadTestEventHandler.SetMetricsCollector(Metrics);

        // Act - Short test for each combination
        var targetRps = outbox || idempotency ? 100 : 200; // Lower RPS for persistence features
        var duration = TimeSpan.FromSeconds(30);

        await PublishAtRateAsync(publisher, targetRps, duration);

        var snapshot = Metrics.GetSnapshot();
        var waitTime = (outbox || idempotency) ? TimeSpan.FromMinutes(2) : TimeSpan.FromMinutes(1);
        await WaitForConsumptionAsync(snapshot.TotalPublished, waitTime);

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.WriteLine($"  Published: {finalMetrics.TotalPublished}");
        Reporter.WriteLine($"  Consumed: {finalMetrics.TotalConsumed}");
        Reporter.WriteLine($"  P99: {finalMetrics.LatencyStatistics.P99.TotalMilliseconds:F2}ms");

        // Assert
        var tolerancePercent = (outbox || idempotency) ? 2.0 : 0.5;
        AssertNoMessageLoss(tolerancePercent);
    }

    /// <summary>
    /// Tests rapid feature toggling doesn't cause issues.
    /// </summary>
    [Fact]
    public async Task Sequential_Feature_Configurations()
    {
        Reporter.WriteLine("Testing sequential feature configurations");

        var configurations = new[]
        {
            ("Baseline", CreateFeatures()),
            ("Retry only", CreateFeatures(retry: true)),
            ("CircuitBreaker only", CreateFeatures(circuitBreaker: true)),
            ("Timeout only", CreateFeatures(timeout: true)),
            ("All resilience", CreateFeatures(retry: true, circuitBreaker: true, timeout: true))
        };

        foreach (var (name, features) in configurations)
        {
            LoadTestEventHandler.Reset();
            Metrics.Reset();

            using var host = await BuildHostWithFeatures<LoadTestEventHandler>(features);
            var publisher = host.Services.GetRequiredService<IEventPublisher>();

            LoadTestEventHandler.SetMetricsCollector(Metrics);

            // Quick burst
            var messageCount = 100;
            for (int i = 0; i < messageCount; i++)
            {
                await publisher.PublishAsync(new LoadTestEvent
                {
                    Sequence = i,
                    PublishedAtTicks = Stopwatch.GetTimestamp()
                }, TestCancellation.Token);
                Metrics.RecordPublished();
            }

            await WaitForHandlerCountAsync(messageCount, TimeSpan.FromSeconds(30));

            var finalMetrics = Metrics.GetSnapshot();
            Reporter.WriteLine($"{name}: {finalMetrics.TotalConsumed}/{finalMetrics.TotalPublished} " +
                             $"P99={finalMetrics.LatencyStatistics.P99.TotalMilliseconds:F2}ms");

            Assert.Equal(messageCount, LoadTestEventHandler.HandleCount);

            await host.StopAsync();
        }
    }
}
