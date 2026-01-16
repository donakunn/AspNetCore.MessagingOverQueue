using System.Collections.Concurrent;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Metrics;

/// <summary>
/// Immutable statistics calculated from latency measurements.
/// </summary>
public sealed record LatencyStatistics
{
    public int Count { get; init; }
    public TimeSpan Min { get; init; }
    public TimeSpan Max { get; init; }
    public TimeSpan Mean { get; init; }
    public TimeSpan P50 { get; init; }
    public TimeSpan P95 { get; init; }
    public TimeSpan P99 { get; init; }
    public TimeSpan P999 { get; init; }

    public static LatencyStatistics Empty => new()
    {
        Count = 0,
        Min = TimeSpan.Zero,
        Max = TimeSpan.Zero,
        Mean = TimeSpan.Zero,
        P50 = TimeSpan.Zero,
        P95 = TimeSpan.Zero,
        P99 = TimeSpan.Zero,
        P999 = TimeSpan.Zero
    };
}

/// <summary>
/// Thread-safe histogram for latency measurements.
/// Uses a concurrent bag for lock-free collection and computes percentiles on demand.
/// </summary>
public sealed class LatencyHistogram
{
    private readonly ConcurrentBag<long> _latenciesMs = new();

    /// <summary>
    /// Records a latency measurement.
    /// Thread-safe.
    /// </summary>
    public void Record(TimeSpan latency)
    {
        _latenciesMs.Add((long)latency.TotalMilliseconds);
    }

    /// <summary>
    /// Gets the number of recorded samples.
    /// </summary>
    public int Count => _latenciesMs.Count;

    /// <summary>
    /// Computes statistics from all recorded latencies.
    /// </summary>
    public LatencyStatistics GetStatistics()
    {
        var sorted = _latenciesMs.OrderBy(x => x).ToArray();
        if (sorted.Length == 0)
        {
            return LatencyStatistics.Empty;
        }

        return new LatencyStatistics
        {
            Count = sorted.Length,
            Min = TimeSpan.FromMilliseconds(sorted[0]),
            Max = TimeSpan.FromMilliseconds(sorted[^1]),
            Mean = TimeSpan.FromMilliseconds(sorted.Average()),
            P50 = TimeSpan.FromMilliseconds(GetPercentile(sorted, 50)),
            P95 = TimeSpan.FromMilliseconds(GetPercentile(sorted, 95)),
            P99 = TimeSpan.FromMilliseconds(GetPercentile(sorted, 99)),
            P999 = TimeSpan.FromMilliseconds(GetPercentile(sorted, 99.9))
        };
    }

    /// <summary>
    /// Clears all recorded latencies.
    /// </summary>
    public void Clear()
    {
        _latenciesMs.Clear();
    }

    private static long GetPercentile(long[] sorted, double percentile)
    {
        if (sorted.Length == 0) return 0;
        var index = (int)Math.Ceiling(percentile / 100.0 * sorted.Length) - 1;
        return sorted[Math.Max(0, Math.Min(index, sorted.Length - 1))];
    }
}
