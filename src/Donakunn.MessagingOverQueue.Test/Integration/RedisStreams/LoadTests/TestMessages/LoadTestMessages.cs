using System.Collections.Concurrent;
using System.Diagnostics;
using Donakunn.MessagingOverQueue.Abstractions.Consuming;
using Donakunn.MessagingOverQueue.Abstractions.Messages;
using Donakunn.MessagingOverQueue.Topology.Attributes;
using MessagingOverQueue.Test.Integration.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Metrics;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.TestMessages;

#region Test Events

/// <summary>
/// Event type for load testing with timing instrumentation.
/// </summary>
public class LoadTestEvent : Event
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
public class SlowLoadTestEvent : Event
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
    /// </summary>
    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
        context.SetCustomData(LatenciesKey, new ConcurrentDictionary<long, long>());
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
