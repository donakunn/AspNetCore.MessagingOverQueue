using System.Collections.Concurrent;
using System.Diagnostics;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Metrics;

/// <summary>
/// Aggregates all metrics for a load test run.
/// Thread-safe implementation for concurrent access from publishers and consumers.
/// </summary>
public sealed class MetricsAggregator : IMetricsCollector
{
    private readonly LatencyHistogram _latencyHistogram = new();
    private readonly ThroughputCounter _publishedCounter;
    private readonly ThroughputCounter _consumedCounter;
    private readonly ConcurrentDictionary<string, long> _errorCounts = new();
    private readonly Stopwatch _testDuration = new();

    /// <summary>
    /// Creates a new metrics aggregator with the specified sampling window.
    /// </summary>
    /// <param name="samplingWindow">The time window for throughput rate sampling.</param>
    public MetricsAggregator(TimeSpan samplingWindow)
    {
        _publishedCounter = new ThroughputCounter(samplingWindow);
        _consumedCounter = new ThroughputCounter(samplingWindow);
    }

    /// <summary>
    /// Gets whether the test timer is running.
    /// </summary>
    public bool IsRunning => _testDuration.IsRunning;

    /// <summary>
    /// Gets the elapsed test duration.
    /// </summary>
    public TimeSpan Elapsed => _testDuration.Elapsed;

    /// <summary>
    /// Starts the test duration timer.
    /// </summary>
    public void Start() => _testDuration.Start();

    /// <summary>
    /// Stops the test duration timer.
    /// </summary>
    public void Stop() => _testDuration.Stop();

    /// <inheritdoc />
    public void RecordLatency(TimeSpan latency) => _latencyHistogram.Record(latency);

    /// <inheritdoc />
    public void RecordPublished() => _publishedCounter.Increment();

    /// <inheritdoc />
    public void RecordConsumed() => _consumedCounter.Increment();

    /// <inheritdoc />
    public void RecordError(string errorType)
    {
        _errorCounts.AddOrUpdate(errorType, 1, (_, count) => count + 1);
    }

    /// <inheritdoc />
    public LoadTestMetrics GetSnapshot()
    {
        var totalPublished = _publishedCounter.TotalCount;
        var totalConsumed = _consumedCounter.TotalCount;

        return new LoadTestMetrics
        {
            TestDuration = _testDuration.Elapsed,
            TotalPublished = totalPublished,
            TotalConsumed = totalConsumed,
            PublishRatePerSecond = _publishedCounter.GetAverageRatePerSecond(),
            ConsumeRatePerSecond = _consumedCounter.GetAverageRatePerSecond(),
            LatencyStatistics = _latencyHistogram.GetStatistics(),
            ErrorCounts = new Dictionary<string, long>(_errorCounts),
            MessageLossCount = Math.Max(0, totalPublished - totalConsumed)
        };
    }

    /// <inheritdoc />
    public void Reset()
    {
        _latencyHistogram.Clear();
        _publishedCounter.Reset();
        _consumedCounter.Reset();
        _errorCounts.Clear();
        _testDuration.Reset();
    }
}
