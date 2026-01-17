namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Metrics;

/// <summary>
/// Immutable snapshot of load test metrics at a point in time.
/// </summary>
public sealed record LoadTestMetrics
{
    /// <summary>
    /// Total duration of the test run.
    /// </summary>
    public TimeSpan TestDuration { get; init; }

    /// <summary>
    /// Total number of messages published.
    /// </summary>
    public long TotalPublished { get; init; }

    /// <summary>
    /// Total number of messages consumed.
    /// </summary>
    public long TotalConsumed { get; init; }

    /// <summary>
    /// Average publish rate in messages per second.
    /// </summary>
    public double PublishRatePerSecond { get; init; }

    /// <summary>
    /// Average consume rate in messages per second.
    /// </summary>
    public double ConsumeRatePerSecond { get; init; }

    /// <summary>
    /// Latency statistics including percentiles.
    /// </summary>
    public LatencyStatistics LatencyStatistics { get; init; } = LatencyStatistics.Empty;

    /// <summary>
    /// Counts of errors by error type.
    /// </summary>
    public IReadOnlyDictionary<string, long> ErrorCounts { get; init; } = new Dictionary<string, long>();

    /// <summary>
    /// Number of messages lost (published but not consumed).
    /// </summary>
    public long MessageLossCount { get; init; }

    /// <summary>
    /// Percentage of messages lost.
    /// </summary>
    public double MessageLossPercentage => TotalPublished > 0
        ? (double)MessageLossCount / TotalPublished * 100
        : 0;

    /// <summary>
    /// Total number of errors across all error types.
    /// </summary>
    public long TotalErrors => ErrorCounts.Values.Sum();

    /// <summary>
    /// Creates an empty metrics snapshot.
    /// </summary>
    public static LoadTestMetrics Empty => new()
    {
        TestDuration = TimeSpan.Zero,
        TotalPublished = 0,
        TotalConsumed = 0,
        PublishRatePerSecond = 0,
        ConsumeRatePerSecond = 0,
        LatencyStatistics = LatencyStatistics.Empty,
        ErrorCounts = new Dictionary<string, long>(),
        MessageLossCount = 0
    };
}
