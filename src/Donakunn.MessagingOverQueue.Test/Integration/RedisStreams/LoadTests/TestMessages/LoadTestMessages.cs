using System.Collections.Concurrent;
using System.Diagnostics;
using Donakunn.MessagingOverQueue.Abstractions.Consuming;
using Donakunn.MessagingOverQueue.Abstractions.Messages;
using Donakunn.MessagingOverQueue.Topology.Attributes;
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
/// Uses static counters for test assertions similar to existing test handlers.
/// </summary>
[ConsumerQueue(MaxConcurrency = 50)]
public class LoadTestEventHandler : IMessageHandler<LoadTestEvent>
{
    private static readonly ConcurrentDictionary<long, long> _latencies = new();
    private static long _handleCount;
    private static IMetricsCollector? _metricsCollector;

    /// <summary>
    /// Gets the total number of messages handled.
    /// </summary>
    public static long HandleCount => Volatile.Read(ref _handleCount);

    /// <summary>
    /// Gets all recorded latencies by sequence number.
    /// </summary>
    public static IReadOnlyDictionary<long, long> Latencies => _latencies;

    /// <summary>
    /// Sets the metrics collector for recording metrics during tests.
    /// </summary>
    public static void SetMetricsCollector(IMetricsCollector? collector)
    {
        _metricsCollector = collector;
    }

    /// <summary>
    /// Resets all static state for test isolation.
    /// </summary>
    public static void Reset()
    {
        Interlocked.Exchange(ref _handleCount, 0);
        _latencies.Clear();
        _metricsCollector = null;
    }

    /// <summary>
    /// Waits for the handler to reach the expected count.
    /// </summary>
    public static async Task WaitForCountAsync(long expected, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (HandleCount < expected && sw.Elapsed < timeout)
        {
            await Task.Delay(50);
        }
    }

    public Task HandleAsync(LoadTestEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        var receivedTicks = Stopwatch.GetTimestamp();
        var latencyTicks = receivedTicks - message.PublishedAtTicks;
        var latency = TimeSpan.FromTicks(latencyTicks * TimeSpan.TicksPerSecond / Stopwatch.Frequency);

        _latencies.TryAdd(message.Sequence, latencyTicks);
        Interlocked.Increment(ref _handleCount);

        _metricsCollector?.RecordLatency(latency);
        _metricsCollector?.RecordConsumed();

        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler for slow load test events (stress testing).
/// Simulates slow processing with configurable delay.
/// </summary>
[ConsumerQueue(MaxConcurrency = 10)]
public class SlowLoadTestEventHandler : IMessageHandler<SlowLoadTestEvent>
{
    private static long _handleCount;
    private static IMetricsCollector? _metricsCollector;

    /// <summary>
    /// Gets the total number of messages handled.
    /// </summary>
    public static long HandleCount => Volatile.Read(ref _handleCount);

    /// <summary>
    /// Sets the metrics collector for recording metrics during tests.
    /// </summary>
    public static void SetMetricsCollector(IMetricsCollector? collector)
    {
        _metricsCollector = collector;
    }

    /// <summary>
    /// Resets all static state for test isolation.
    /// </summary>
    public static void Reset()
    {
        Interlocked.Exchange(ref _handleCount, 0);
        _metricsCollector = null;
    }

    /// <summary>
    /// Waits for the handler to reach the expected count.
    /// </summary>
    public static async Task WaitForCountAsync(long expected, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (HandleCount < expected && sw.Elapsed < timeout)
        {
            await Task.Delay(100);
        }
    }

    public async Task HandleAsync(SlowLoadTestEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        var receivedTicks = Stopwatch.GetTimestamp();

        // Simulate slow processing
        await Task.Delay(message.ProcessingDelay, cancellationToken);

        var latencyTicks = receivedTicks - message.PublishedAtTicks;
        var latency = TimeSpan.FromTicks(latencyTicks * TimeSpan.TicksPerSecond / Stopwatch.Frequency);

        Interlocked.Increment(ref _handleCount);

        _metricsCollector?.RecordLatency(latency);
        _metricsCollector?.RecordConsumed();
    }
}

#endregion
