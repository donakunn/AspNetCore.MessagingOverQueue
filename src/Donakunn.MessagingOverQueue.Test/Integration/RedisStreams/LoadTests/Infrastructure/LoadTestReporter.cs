using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Metrics;
using Xunit.Abstractions;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Infrastructure;

/// <summary>
/// Generates human-readable test reports.
/// Outputs to xUnit ITestOutputHelper for integration with test runners.
/// </summary>
public sealed class LoadTestReporter
{
    private readonly ITestOutputHelper _output;
    private readonly List<LoadTestMetrics> _snapshots = [];

    /// <summary>
    /// Creates a new reporter with the specified output helper.
    /// </summary>
    public LoadTestReporter(ITestOutputHelper output)
    {
        _output = output ?? throw new ArgumentNullException(nameof(output));
    }

    /// <summary>
    /// Gets all recorded metrics snapshots.
    /// </summary>
    public IReadOnlyList<LoadTestMetrics> Snapshots => _snapshots;

    /// <summary>
    /// Records a metrics snapshot for later analysis.
    /// </summary>
    public void RecordSnapshot(LoadTestMetrics metrics)
    {
        _snapshots.Add(metrics);
    }

    /// <summary>
    /// Reports current progress during test execution.
    /// </summary>
    public void ReportProgress(LoadTestMetrics metrics, string phase = "Running")
    {
        _output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] {phase}");
        _output.WriteLine($"  Duration: {metrics.TestDuration:hh\\:mm\\:ss}");
        _output.WriteLine($"  Published: {metrics.TotalPublished:N0} ({metrics.PublishRatePerSecond:N1}/sec)");
        _output.WriteLine($"  Consumed: {metrics.TotalConsumed:N0} ({metrics.ConsumeRatePerSecond:N1}/sec)");

        if (metrics.LatencyStatistics.Count > 0)
        {
            _output.WriteLine($"  P99 Latency: {metrics.LatencyStatistics.P99.TotalMilliseconds:N1}ms");
        }

        if (metrics.TotalErrors > 0)
        {
            _output.WriteLine($"  Errors: {metrics.TotalErrors:N0}");
        }

        _output.WriteLine("");
    }

    /// <summary>
    /// Reports a message to the test output.
    /// </summary>
    public void WriteLine(string message)
    {
        _output.WriteLine(message);
    }

    /// <summary>
    /// Reports the final test results with detailed statistics.
    /// </summary>
    public void ReportFinal(LoadTestMetrics metrics, string testName)
    {
        _output.WriteLine("");
        _output.WriteLine($"========== {testName} Final Report ==========");
        _output.WriteLine($"Test Duration: {metrics.TestDuration:hh\\:mm\\:ss\\.fff}");
        _output.WriteLine("");

        // Throughput section
        _output.WriteLine("THROUGHPUT:");
        _output.WriteLine($"  Total Published:  {metrics.TotalPublished:N0}");
        _output.WriteLine($"  Total Consumed:   {metrics.TotalConsumed:N0}");
        _output.WriteLine($"  Avg Publish Rate: {metrics.PublishRatePerSecond:N2} msg/sec");
        _output.WriteLine($"  Avg Consume Rate: {metrics.ConsumeRatePerSecond:N2} msg/sec");
        _output.WriteLine("");

        // Latency section
        if (metrics.LatencyStatistics.Count > 0)
        {
            _output.WriteLine("LATENCY:");
            _output.WriteLine($"  Samples: {metrics.LatencyStatistics.Count:N0}");
            _output.WriteLine($"  Min:     {metrics.LatencyStatistics.Min.TotalMilliseconds:N2}ms");
            _output.WriteLine($"  Mean:    {metrics.LatencyStatistics.Mean.TotalMilliseconds:N2}ms");
            _output.WriteLine($"  P50:     {metrics.LatencyStatistics.P50.TotalMilliseconds:N2}ms");
            _output.WriteLine($"  P95:     {metrics.LatencyStatistics.P95.TotalMilliseconds:N2}ms");
            _output.WriteLine($"  P99:     {metrics.LatencyStatistics.P99.TotalMilliseconds:N2}ms");
            _output.WriteLine($"  P99.9:   {metrics.LatencyStatistics.P999.TotalMilliseconds:N2}ms");
            _output.WriteLine($"  Max:     {metrics.LatencyStatistics.Max.TotalMilliseconds:N2}ms");
            _output.WriteLine("");
        }

        // Reliability section
        _output.WriteLine("RELIABILITY:");
        _output.WriteLine($"  Message Loss: {metrics.MessageLossCount:N0} ({metrics.MessageLossPercentage:N4}%)");

        // Errors section
        if (metrics.ErrorCounts.Count > 0)
        {
            _output.WriteLine("");
            _output.WriteLine("ERRORS:");
            foreach (var (errorType, count) in metrics.ErrorCounts.OrderByDescending(e => e.Value))
            {
                _output.WriteLine($"  {errorType}: {count:N0}");
            }
        }

        _output.WriteLine("==============================================");
        _output.WriteLine("");
    }

    /// <summary>
    /// Reports a comparison between expected and actual values.
    /// </summary>
    public void ReportComparison(string metric, double expected, double actual, string unit = "")
    {
        var percentDiff = expected > 0 ? ((actual - expected) / expected) * 100 : 0;
        var status = actual >= expected ? "PASS" : "FAIL";
        _output.WriteLine($"  {metric}: {actual:N2}{unit} (expected: {expected:N2}{unit}, diff: {percentDiff:+0.0;-0.0}%) [{status}]");
    }
}
