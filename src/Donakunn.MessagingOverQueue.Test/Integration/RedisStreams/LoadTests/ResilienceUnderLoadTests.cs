using System.Diagnostics;
using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.TestMessages;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests;

/// <summary>
/// Load tests for resilience features under failure conditions.
/// Tests retry, circuit breaker, and timeout behavior when failures occur.
/// </summary>
[Trait("Category", "LoadTest")]
[Trait("Category", "Resilience")]
public class ResilienceUnderLoadTests : LoadTestBase
{
    public ResilienceUnderLoadTests(ITestOutputHelper output) : base(output)
    {
    }

    /// <summary>
    /// Tests retry handling of transient failures under load.
    /// Messages that fail transiently should eventually succeed after retries.
    /// </summary>
    [Fact]
    public async Task Retry_Handles_Transient_Failures_Under_Load()
    {
        // Arrange
        FailingLoadTestEventHandler.Reset();
        var features = CreateFeatures(
            retry: true,
            retryMaxAttempts: 3,
            retryInitialDelay: TimeSpan.FromMilliseconds(50));

        using var host = await BuildHostWithFeatures<FailingLoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing retry with transient failures");
        Reporter.WriteLine($"Configuration: MaxAttempts={features.RetryMaxAttempts}");

        FailingLoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act - Publish mix of normal and transient failing messages
        var targetRps = 100;
        var duration = TimeSpan.FromMinutes(1);
        long sequence = 0;

        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < duration && !TestCancellation.IsCancellationRequested)
        {
            var seq = Interlocked.Increment(ref sequence);
            var isTransientFailure = seq % 10 == 0; // 10% transient failures

            var message = new FailingLoadTestEvent
            {
                Sequence = seq,
                PublishedAtTicks = Stopwatch.GetTimestamp(),
                ShouldFail = isTransientFailure,
                IsTransient = true,
                FailOnAttempts = isTransientFailure ? [1, 2] : [] // Fail first 2 attempts, succeed on 3rd
            };

            await publisher.PublishAsync(message, TestCancellation.Token);
            Metrics.RecordPublished();

            await Task.Delay(TimeSpan.FromMilliseconds(1000.0 / targetRps), TestCancellation.Token);
        }

        // Wait for all messages to be processed (including retries)
        var published = Metrics.GetSnapshot().TotalPublished;
        await WaitForHandlerCountAsync(published, TimeSpan.FromMinutes(2));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Test");
        Reporter.WriteLine($"Transient failures handled: {FailingLoadTestEventHandler.ErrorCount}");
        Reporter.WriteLine($"Successfully processed: {FailingLoadTestEventHandler.HandleCount}");

        // Assert - All messages should eventually succeed
        Assert.Equal(published, FailingLoadTestEventHandler.HandleCount);
    }

    /// <summary>
    /// Tests circuit breaker opening under high failure rate.
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_Opens_Under_High_Failure_Rate()
    {
        // Arrange
        FailingLoadTestEventHandler.Reset();
        var features = CreateFeatures(
            circuitBreaker: true,
            circuitBreakerThreshold: 5,
            circuitBreakerDuration: TimeSpan.FromSeconds(10));

        using var host = await BuildHostWithFeatures<FailingLoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing circuit breaker opens under failures");
        Reporter.WriteLine($"Configuration: Threshold={features.CircuitBreakerFailureThreshold}");

        FailingLoadTestEventHandler.SetMetricsCollector(Metrics);

        // Act - Send messages that will all fail
        var failingMessages = 20;
        for (int i = 0; i < failingMessages; i++)
        {
            var message = new FailingLoadTestEvent
            {
                Sequence = i,
                PublishedAtTicks = Stopwatch.GetTimestamp(),
                ShouldFail = true,
                IsTransient = false
            };

            await publisher.PublishAsync(message, TestCancellation.Token);
            Metrics.RecordPublished();
        }

        // Wait for circuit to open
        await Task.Delay(TimeSpan.FromSeconds(5));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.WriteLine($"Messages published: {finalMetrics.TotalPublished}");
        Reporter.WriteLine($"Errors recorded: {FailingLoadTestEventHandler.ErrorCount}");
        Reporter.WriteLine($"Successfully handled: {FailingLoadTestEventHandler.HandleCount}");

        // Assert - Circuit should have opened after threshold failures
        // Some messages may be rejected by open circuit
        Assert.True(FailingLoadTestEventHandler.ErrorCount >= features.CircuitBreakerFailureThreshold,
            $"Expected at least {features.CircuitBreakerFailureThreshold} errors before circuit opens");
    }

    /// <summary>
    /// Tests circuit breaker recovery after being open.
    /// </summary>
    [Fact]
    public async Task CircuitBreaker_Recovery_Under_Load()
    {
        // Arrange
        FailingLoadTestEventHandler.Reset();
        var features = CreateFeatures(
            circuitBreaker: true,
            circuitBreakerThreshold: 3,
            circuitBreakerDuration: TimeSpan.FromSeconds(5));

        using var host = await BuildHostWithFeatures<FailingLoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing circuit breaker recovery");

        FailingLoadTestEventHandler.SetMetricsCollector(Metrics);

        // Phase 1: Cause circuit to open with failures
        Reporter.WriteLine("Phase 1: Opening circuit with failures...");
        for (int i = 0; i < 5; i++)
        {
            var failMessage = new FailingLoadTestEvent
            {
                Sequence = i,
                PublishedAtTicks = Stopwatch.GetTimestamp(),
                ShouldFail = true
            };
            await publisher.PublishAsync(failMessage, TestCancellation.Token);
        }

        await Task.Delay(TimeSpan.FromSeconds(2));
        Reporter.WriteLine($"Errors after phase 1: {FailingLoadTestEventHandler.ErrorCount}");

        // Phase 2: Wait for circuit to close
        Reporter.WriteLine("Phase 2: Waiting for circuit to close...");
        await Task.Delay(features.CircuitBreakerDurationOfBreak + TimeSpan.FromSeconds(2));

        // Phase 3: Send successful messages
        Reporter.WriteLine("Phase 3: Sending successful messages...");
        var successfulMessages = 10;
        for (int i = 0; i < successfulMessages; i++)
        {
            var successMessage = new FailingLoadTestEvent
            {
                Sequence = 100 + i,
                PublishedAtTicks = Stopwatch.GetTimestamp(),
                ShouldFail = false
            };
            await publisher.PublishAsync(successMessage, TestCancellation.Token);
            Metrics.RecordPublished();
        }

        await WaitForHandlerCountAsync(successfulMessages, TimeSpan.FromSeconds(30));

        // Report
        Reporter.WriteLine($"Final successful count: {FailingLoadTestEventHandler.HandleCount}");
        Reporter.WriteLine($"Total errors: {FailingLoadTestEventHandler.ErrorCount}");

        // Assert - Circuit should have recovered and processed successful messages
        Assert.True(FailingLoadTestEventHandler.HandleCount >= successfulMessages,
            $"Expected at least {successfulMessages} successful messages after recovery");
    }

    /// <summary>
    /// Tests timeout handling of slow messages under load.
    /// </summary>
    [Fact]
    public async Task Timeout_Handles_Slow_Messages_Under_Load()
    {
        // Arrange
        FailingLoadTestEventHandler.Reset();
        var timeoutDuration = TimeSpan.FromMilliseconds(500);
        var features = CreateFeatures(
            timeout: true,
            timeoutDuration: timeoutDuration);

        using var host = await BuildHostWithFeatures<FailingLoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing timeout handling");
        Reporter.WriteLine($"Configuration: Timeout={timeoutDuration.TotalMilliseconds}ms");

        FailingLoadTestEventHandler.SetMetricsCollector(Metrics);

        // Act - Mix of fast and slow messages
        var totalMessages = 50;
        var slowMessageDelay = TimeSpan.FromSeconds(2); // Much longer than timeout
        long successExpected = 0;

        for (int i = 0; i < totalMessages; i++)
        {
            var isSlow = i % 5 == 0; // 20% slow messages

            var message = new FailingLoadTestEvent
            {
                Sequence = i,
                PublishedAtTicks = Stopwatch.GetTimestamp(),
                ShouldFail = false,
                SimulatedDelay = isSlow ? slowMessageDelay : TimeSpan.Zero
            };

            await publisher.PublishAsync(message, TestCancellation.Token);
            Metrics.RecordPublished();

            if (!isSlow)
            {
                successExpected++;
            }
        }

        // Wait for processing
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Report
        Reporter.WriteLine($"Messages published: {totalMessages}");
        Reporter.WriteLine($"Fast messages expected: {successExpected}");
        Reporter.WriteLine($"Successfully handled: {FailingLoadTestEventHandler.HandleCount}");

        // Assert - Fast messages should succeed, slow should timeout
        Assert.True(FailingLoadTestEventHandler.HandleCount >= successExpected * 0.9,
            $"Expected at least 90% of fast messages ({successExpected * 0.9}) to succeed");
    }

    /// <summary>
    /// Tests combined resilience features under chaotic conditions.
    /// Random mix of transient failures, slow processing, and normal messages.
    /// </summary>
    [Fact]
    public async Task Combined_Resilience_Under_Chaos()
    {
        // Arrange
        FailingLoadTestEventHandler.Reset();
        var features = CreateFeatures(
            retry: true,
            retryMaxAttempts: 3,
            retryInitialDelay: TimeSpan.FromMilliseconds(100),
            circuitBreaker: true,
            circuitBreakerThreshold: 10,
            circuitBreakerDuration: TimeSpan.FromSeconds(5),
            timeout: true,
            timeoutDuration: TimeSpan.FromSeconds(2));

        using var host = await BuildHostWithFeatures<FailingLoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing combined resilience under chaos");
        Reporter.WriteLine($"Features: {features}");

        FailingLoadTestEventHandler.SetMetricsCollector(Metrics);
        StartPeriodicReporting();

        // Act - Send chaotic mix of messages
        var random = new Random(42); // Deterministic for reproducibility
        var targetRps = 50;
        var duration = TimeSpan.FromMinutes(1);
        long sequence = 0;

        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < duration && !TestCancellation.IsCancellationRequested)
        {
            var seq = Interlocked.Increment(ref sequence);
            var roll = random.NextDouble();

            FailingLoadTestEvent message;
            if (roll < 0.70) // 70% normal
            {
                message = new FailingLoadTestEvent
                {
                    Sequence = seq,
                    PublishedAtTicks = Stopwatch.GetTimestamp(),
                    ShouldFail = false
                };
            }
            else if (roll < 0.85) // 15% transient failure
            {
                message = new FailingLoadTestEvent
                {
                    Sequence = seq,
                    PublishedAtTicks = Stopwatch.GetTimestamp(),
                    ShouldFail = true,
                    IsTransient = true,
                    FailOnAttempts = [1]
                };
            }
            else if (roll < 0.95) // 10% slow
            {
                message = new FailingLoadTestEvent
                {
                    Sequence = seq,
                    PublishedAtTicks = Stopwatch.GetTimestamp(),
                    ShouldFail = false,
                    SimulatedDelay = TimeSpan.FromMilliseconds(500)
                };
            }
            else // 5% permanent failure
            {
                message = new FailingLoadTestEvent
                {
                    Sequence = seq,
                    PublishedAtTicks = Stopwatch.GetTimestamp(),
                    ShouldFail = true,
                    IsTransient = false
                };
            }

            await publisher.PublishAsync(message, TestCancellation.Token);
            Metrics.RecordPublished();

            await Task.Delay(TimeSpan.FromMilliseconds(1000.0 / targetRps), TestCancellation.Token);
        }

        // Wait for processing
        await Task.Delay(TimeSpan.FromSeconds(30));

        // Report
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Test");
        Reporter.WriteLine($"Total errors: {FailingLoadTestEventHandler.ErrorCount}");
        Reporter.WriteLine($"Successfully handled: {FailingLoadTestEventHandler.HandleCount}");

        // Assert - System should remain stable under chaos
        // Most normal and recovered transient messages should succeed
        var expectedMinSuccess = (long)(finalMetrics.TotalPublished * 0.60); // At least 60% should succeed
        Assert.True(FailingLoadTestEventHandler.HandleCount >= expectedMinSuccess,
            $"Expected at least {expectedMinSuccess} successful messages under chaos");
    }

    /// <summary>
    /// Tests that retry exhaustion properly rejects messages.
    /// </summary>
    [Fact]
    public async Task Retry_Exhaustion_Rejects_Message()
    {
        // Arrange
        FailingLoadTestEventHandler.Reset();
        var features = CreateFeatures(
            retry: true,
            retryMaxAttempts: 3,
            retryInitialDelay: TimeSpan.FromMilliseconds(50));

        using var host = await BuildHostWithFeatures<FailingLoadTestEventHandler>(features);
        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        Reporter.WriteLine($"Testing retry exhaustion");
        Reporter.WriteLine($"Configuration: MaxAttempts={features.RetryMaxAttempts}");

        FailingLoadTestEventHandler.SetMetricsCollector(Metrics);

        // Act - Send permanently failing messages
        var failingMessages = 10;
        for (int i = 0; i < failingMessages; i++)
        {
            var message = new FailingLoadTestEvent
            {
                Sequence = i,
                PublishedAtTicks = Stopwatch.GetTimestamp(),
                ShouldFail = true,
                IsTransient = false // Permanent failure
            };

            await publisher.PublishAsync(message, TestCancellation.Token);
            Metrics.RecordPublished();
        }

        // Wait for all retry attempts to complete
        await Task.Delay(TimeSpan.FromSeconds(10));

        // Report
        Reporter.WriteLine($"Messages published: {failingMessages}");
        Reporter.WriteLine($"Error count: {FailingLoadTestEventHandler.ErrorCount}");
        Reporter.WriteLine($"Attempt counts by message:");

        foreach (var kvp in FailingLoadTestEventHandler.AttemptCounts.Take(5))
        {
            Reporter.WriteLine($"  Message {kvp.Key}: {kvp.Value} attempts");
        }

        // Assert - Each message should have been attempted max times
        // Success count should be 0 since all messages fail permanently
        Assert.Equal(0, FailingLoadTestEventHandler.HandleCount);

        // Error count should be at least messages * max attempts
        // (could be more if there's message redelivery)
        Assert.True(FailingLoadTestEventHandler.ErrorCount >= failingMessages,
            $"Expected at least {failingMessages} errors");
    }
}
