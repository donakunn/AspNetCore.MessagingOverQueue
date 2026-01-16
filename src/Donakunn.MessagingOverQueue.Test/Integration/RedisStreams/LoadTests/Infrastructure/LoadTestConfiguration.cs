namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Infrastructure;

/// <summary>
/// Configuration for load tests. Values can be overridden via environment variables
/// for CI/CD flexibility. Environment variable names follow pattern: LOADTEST_{PropertyName}.
/// </summary>
public sealed class LoadTestConfiguration
{
    #region Throughput Test Settings

    /// <summary>
    /// Duration for sustained throughput tests.
    /// Default: 10 minutes. Override: LOADTEST_THROUGHPUT_DURATION
    /// </summary>
    public TimeSpan ThroughputTestDuration { get; init; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Target messages per second for throughput tests.
    /// Default: 1000. Override: LOADTEST_THROUGHPUT_TARGET_RPS
    /// </summary>
    public int ThroughputTargetMessagesPerSecond { get; init; } = 1000;

    #endregion

    #region Latency Test Settings

    /// <summary>
    /// Load levels (messages/second) to test for latency measurements.
    /// Default: [100, 1000, 10000]. Override: LOADTEST_LATENCY_LEVELS (comma-separated)
    /// </summary>
    public int[] LatencyTestLoadLevels { get; init; } = [100, 1000, 10000];

    /// <summary>
    /// Duration to run at each load level for latency tests.
    /// Default: 2 minutes. Override: LOADTEST_LATENCY_DURATION_PER_LEVEL
    /// </summary>
    public TimeSpan LatencyTestDurationPerLevel { get; init; } = TimeSpan.FromMinutes(2);

    #endregion

    #region Recovery Test Settings

    /// <summary>
    /// Number of pending messages to accumulate before consumer restart.
    /// Default: 10000. Override: LOADTEST_RECOVERY_PENDING_COUNT
    /// </summary>
    public int RecoveryTestPendingMessageCount { get; init; } = 10000;

    /// <summary>
    /// Maximum time allowed for consumer to recover and process all pending messages.
    /// Default: 5 minutes. Override: LOADTEST_RECOVERY_TIMEOUT
    /// </summary>
    public TimeSpan RecoveryTestTimeout { get; init; } = TimeSpan.FromMinutes(5);

    #endregion

    #region Stress Test Settings

    /// <summary>
    /// Duration for stress tests where consumer is slower than publisher.
    /// Default: 1 hour. Override: LOADTEST_STRESS_DURATION
    /// </summary>
    public TimeSpan StressTestDuration { get; init; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Ratio of publisher rate to consumer processing capacity.
    /// Default: 2.0 (publisher is 2x faster than consumer). Override: LOADTEST_STRESS_RATIO
    /// </summary>
    public double StressTestPublisherToConsumerRatio { get; init; } = 2.0;

    /// <summary>
    /// Publisher rate in messages per second for stress tests.
    /// Default: 500. Override: LOADTEST_STRESS_PUBLISHER_RPS
    /// </summary>
    public int StressTestPublisherRatePerSecond { get; init; } = 500;

    /// <summary>
    /// Maximum time to wait for backlog to drain after stress test publishing completes.
    /// Default: 30 minutes. Override: LOADTEST_STRESS_DRAIN_TIMEOUT
    /// </summary>
    public TimeSpan StressTestDrainTimeout { get; init; } = TimeSpan.FromMinutes(30);

    #endregion

    #region Common Settings

    /// <summary>
    /// Number of messages to publish during warmup phase.
    /// Default: 100. Override: LOADTEST_WARMUP_COUNT
    /// </summary>
    public int WarmupMessageCount { get; init; } = 100;

    /// <summary>
    /// Interval for periodic metrics reporting during tests.
    /// Default: 30 seconds. Override: LOADTEST_REPORTING_INTERVAL
    /// </summary>
    public TimeSpan MetricsReportingInterval { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Time window for throughput rate sampling.
    /// Default: 1 second.
    /// </summary>
    public TimeSpan MetricsSamplingWindow { get; init; } = TimeSpan.FromSeconds(1);

    #endregion

    /// <summary>
    /// Creates configuration with default values.
    /// </summary>
    public static LoadTestConfiguration Default => new();

    /// <summary>
    /// Creates configuration from environment variables with defaults.
    /// </summary>
    public static LoadTestConfiguration FromEnvironment()
    {
        return new LoadTestConfiguration
        {
            // Throughput
            ThroughputTestDuration = GetEnvTimeSpan("LOADTEST_THROUGHPUT_DURATION", TimeSpan.FromMinutes(10)),
            ThroughputTargetMessagesPerSecond = GetEnvInt("LOADTEST_THROUGHPUT_TARGET_RPS", 1000),

            // Latency
            LatencyTestLoadLevels = GetEnvIntArray("LOADTEST_LATENCY_LEVELS", [100, 1000, 10000]),
            LatencyTestDurationPerLevel = GetEnvTimeSpan("LOADTEST_LATENCY_DURATION_PER_LEVEL", TimeSpan.FromMinutes(2)),

            // Recovery
            RecoveryTestPendingMessageCount = GetEnvInt("LOADTEST_RECOVERY_PENDING_COUNT", 10000),
            RecoveryTestTimeout = GetEnvTimeSpan("LOADTEST_RECOVERY_TIMEOUT", TimeSpan.FromMinutes(5)),

            // Stress
            StressTestDuration = GetEnvTimeSpan("LOADTEST_STRESS_DURATION", TimeSpan.FromHours(1)),
            StressTestPublisherToConsumerRatio = GetEnvDouble("LOADTEST_STRESS_RATIO", 2.0),
            StressTestPublisherRatePerSecond = GetEnvInt("LOADTEST_STRESS_PUBLISHER_RPS", 500),
            StressTestDrainTimeout = GetEnvTimeSpan("LOADTEST_STRESS_DRAIN_TIMEOUT", TimeSpan.FromMinutes(30)),

            // Common
            WarmupMessageCount = GetEnvInt("LOADTEST_WARMUP_COUNT", 100),
            MetricsReportingInterval = GetEnvTimeSpan("LOADTEST_REPORTING_INTERVAL", TimeSpan.FromSeconds(30))
        };
    }

    private static TimeSpan GetEnvTimeSpan(string name, TimeSpan defaultValue)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return TimeSpan.TryParse(value, out var result) ? result : defaultValue;
    }

    private static int GetEnvInt(string name, int defaultValue)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return int.TryParse(value, out var result) ? result : defaultValue;
    }

    private static double GetEnvDouble(string name, double defaultValue)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return double.TryParse(value, out var result) ? result : defaultValue;
    }

    private static int[] GetEnvIntArray(string name, int[] defaultValue)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(value)) return defaultValue;

        try
        {
            return value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(int.Parse)
                .ToArray();
        }
        catch
        {
            return defaultValue;
        }
    }
}
