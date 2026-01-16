namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Metrics;

/// <summary>
/// Interface for collecting metrics during load tests.
/// Implementations must be thread-safe.
/// </summary>
public interface IMetricsCollector
{
    /// <summary>
    /// Records a latency measurement.
    /// </summary>
    void RecordLatency(TimeSpan latency);

    /// <summary>
    /// Records a published message.
    /// </summary>
    void RecordPublished();

    /// <summary>
    /// Records a consumed message.
    /// </summary>
    void RecordConsumed();

    /// <summary>
    /// Records an error occurrence.
    /// </summary>
    void RecordError(string errorType);

    /// <summary>
    /// Gets an immutable snapshot of current metrics.
    /// </summary>
    LoadTestMetrics GetSnapshot();

    /// <summary>
    /// Resets all metrics to initial state.
    /// </summary>
    void Reset();
}
