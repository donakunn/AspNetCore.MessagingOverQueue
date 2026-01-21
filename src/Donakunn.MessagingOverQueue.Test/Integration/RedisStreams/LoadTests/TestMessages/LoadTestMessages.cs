using System.Collections.Concurrent;
using System.Diagnostics;
using Donakunn.MessagingOverQueue.Abstractions.Consuming;
using Donakunn.MessagingOverQueue.Abstractions.Messages;
using Donakunn.MessagingOverQueue.Topology.Attributes;
using MessagingOverQueue.Test.Integration.Shared.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Metrics;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.TestMessages;

#region Test Events

/// <summary>
/// Event type for load testing with timing instrumentation.
/// </summary>
public record LoadTestEvent : Event
{
    /// <summary>
    /// Sequence number for ordering verification.
    /// </summary>
    public long Sequence { get; set; }

    /// <summary>
    /// High-resolution timestamp when the message was published.
    /// Used for precise latency measurement.
    /// </summary>
    public long PublishedAtTicks { get; set; } = Stopwatch.GetTimestamp();

    /// <summary>
    /// Indicates if this message is part of warmup phase.
    /// </summary>
    public bool IsWarmup { get; set; }

    /// <summary>
    /// Optional payload for testing message size impact.
    /// </summary>
    public string? Payload { get; set; }
}

/// <summary>
/// Event for stress testing with configurable processing delay.
/// </summary>
public record SlowLoadTestEvent : Event
{
    /// <summary>
    /// Sequence number for ordering verification.
    /// </summary>
    public long Sequence { get; set; }

    /// <summary>
    /// High-resolution timestamp when the message was published.
    /// </summary>
    public long PublishedAtTicks { get; set; } = Stopwatch.GetTimestamp();

    /// <summary>
    /// Simulated processing delay.
    /// </summary>
    public TimeSpan ProcessingDelay { get; set; } = TimeSpan.FromMilliseconds(50);
}

/// <summary>
/// Event for testing failure injection and resilience features under load.
/// Supports configurable failure patterns for retry, circuit breaker, and timeout testing.
/// </summary>
public record FailingLoadTestEvent : Event
{
    /// <summary>
    /// Sequence number for ordering verification.
    /// </summary>
    public long Sequence { get; set; }

    /// <summary>
    /// High-resolution timestamp when the message was published.
    /// </summary>
    public long PublishedAtTicks { get; set; } = Stopwatch.GetTimestamp();

    /// <summary>
    /// Whether this message's handler should throw an exception.
    /// </summary>
    public bool ShouldFail { get; set; }

    /// <summary>
    /// Whether the failure is transient (recoverable after retries).
    /// When true, the handler will succeed after FailOnAttempts are exhausted.
    /// </summary>
    public bool IsTransient { get; set; }

    /// <summary>
    /// Simulated processing delay before failure/success.
    /// Use for timeout testing.
    /// </summary>
    public TimeSpan SimulatedDelay { get; set; } = TimeSpan.Zero;

    /// <summary>
    /// List of attempt numbers (1-based) on which to fail.
    /// If empty and ShouldFail is true, fails on all attempts.
    /// Example: [1, 2] means fail on first and second attempts, succeed on third.
    /// </summary>
    public List<int> FailOnAttempts { get; set; } = [];

    /// <summary>
    /// Error message to use when throwing.
    /// </summary>
    public string FailureMessage { get; set; } = "Simulated failure for load testing";
}

#endregion

#region Test Handlers

/// <summary>
/// Handler for load test events with latency tracking.
/// Uses TestExecutionContext for test isolation in parallel execution.
/// </summary>
[ConsumerQueue(MaxConcurrency = 50)]
public class LoadTestEventHandler : IMessageHandler<LoadTestEvent>
{
    private const string HandlerKey = nameof(LoadTestEventHandler);
    private const string LatenciesKey = HandlerKey + "_Latencies";
    private const string MetricsCollectorKey = HandlerKey + "_MetricsCollector";

    /// <summary>
    /// Static collection tracking all processed sequences across host restarts.
    /// This is NOT cleared by Reset() - use ResetAll() to clear it.
    /// </summary>
    private static readonly ConcurrentDictionary<long, bool> _processedSequences = new();

    /// <summary>
    /// Gets the total number of unique sequences processed across all hosts.
    /// This persists across Reset() calls.
    /// </summary>
    public static int TotalUniqueProcessed => _processedSequences.Count;

    /// <summary>
    /// Gets all processed sequences.
    /// </summary>
    public static IEnumerable<long> ProcessedSequences => _processedSequences.Keys;

    /// <summary>
    /// Gets the total number of messages handled.
    /// </summary>
    public static long HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;

    /// <summary>
    /// Gets all recorded latencies by sequence number.
    /// </summary>
    public static IReadOnlyDictionary<long, long> Latencies
    {
        get
        {
            var dict = TestExecutionContextAccessor.GetRequired().GetCustomData<ConcurrentDictionary<long, long>>(LatenciesKey);
            return dict ?? new ConcurrentDictionary<long, long>();
        }
    }

    /// <summary>
    /// Sets the metrics collector for recording metrics during tests.
    /// </summary>
    public static void SetMetricsCollector(IMetricsCollector? collector)
    {
        TestExecutionContextAccessor.GetRequired().SetCustomData(MetricsCollectorKey, collector!);
    }

    /// <summary>
    /// Resets counters for test isolation.
    /// Does NOT clear the static processed sequences collection - use ResetAll() for that.
    /// </summary>
    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
        context.SetCustomData(LatenciesKey, new ConcurrentDictionary<long, long>());
    }

    /// <summary>
    /// Resets all state including the static processed sequences collection.
    /// Use this at the start of tests that need complete isolation.
    /// </summary>
    public static void ResetAll()
    {
        Reset();
        _processedSequences.Clear();
    }

    /// <summary>
    /// Waits for the handler to reach the expected count.
    /// </summary>
    public static Task WaitForCountAsync(long expected, TimeSpan timeout)
        => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).WaitForCountAsync((int)expected, timeout);

    public Task HandleAsync(LoadTestEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        var testContext = TestExecutionContextAccessor.GetRequired();

        var receivedTicks = Stopwatch.GetTimestamp();
        var latencyTicks = receivedTicks - message.PublishedAtTicks;
        var latency = TimeSpan.FromTicks(latencyTicks * TimeSpan.TicksPerSecond / Stopwatch.Frequency);

        var latencies = testContext.GetCustomData<ConcurrentDictionary<long, long>>(LatenciesKey);
        latencies?.TryAdd(message.Sequence, latencyTicks);

        // Track processed sequence in static collection (persists across host restarts)
        _processedSequences.TryAdd(message.Sequence, true);

        testContext.GetCounter(HandlerKey).Increment();

        var metricsCollector = testContext.GetCustomData<IMetricsCollector>(MetricsCollectorKey);
        metricsCollector?.RecordLatency(latency);
        metricsCollector?.RecordConsumed();

        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler for slow load test events (stress testing).
/// Simulates slow processing with configurable delay.
/// Uses TestExecutionContext for test isolation in parallel execution.
/// </summary>
[ConsumerQueue(MaxConcurrency = 10)]
public class SlowLoadTestEventHandler : IMessageHandler<SlowLoadTestEvent>
{
    private const string HandlerKey = nameof(SlowLoadTestEventHandler);
    private const string MetricsCollectorKey = HandlerKey + "_MetricsCollector";

    /// <summary>
    /// Gets the total number of messages handled.
    /// </summary>
    public static long HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;

    /// <summary>
    /// Sets the metrics collector for recording metrics during tests.
    /// </summary>
    public static void SetMetricsCollector(IMetricsCollector? collector)
    {
        TestExecutionContextAccessor.GetRequired().SetCustomData(MetricsCollectorKey, collector!);
    }

    /// <summary>
    /// Resets counters for test isolation.
    /// </summary>
    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
    }

    /// <summary>
    /// Waits for the handler to reach the expected count.
    /// </summary>
    public static Task WaitForCountAsync(long expected, TimeSpan timeout)
        => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).WaitForCountAsync((int)expected, timeout);

    public async Task HandleAsync(SlowLoadTestEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        var testContext = TestExecutionContextAccessor.GetRequired();

        var receivedTicks = Stopwatch.GetTimestamp();

        // Simulate slow processing
        await Task.Delay(message.ProcessingDelay, cancellationToken);

        var latencyTicks = receivedTicks - message.PublishedAtTicks;
        var latency = TimeSpan.FromTicks(latencyTicks * TimeSpan.TicksPerSecond / Stopwatch.Frequency);

        testContext.GetCounter(HandlerKey).Increment();

        var metricsCollector = testContext.GetCustomData<IMetricsCollector>(MetricsCollectorKey);
        metricsCollector?.RecordLatency(latency);
        metricsCollector?.RecordConsumed();
    }
}

#endregion
