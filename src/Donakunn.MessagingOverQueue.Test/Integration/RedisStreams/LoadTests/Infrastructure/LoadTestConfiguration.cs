namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Infrastructure;

/// <summary>
/// Configuration for middleware features in load tests.
/// Allows enabling/disabling persistence and resilience features for comparative testing.
/// </summary>
public sealed class FeatureConfiguration
{
    #region Persistence Features

    /// <summary>
    /// Whether the outbox pattern is enabled.
    /// Default: false. Override: LOADTEST_FEATURE_OUTBOX_ENABLED
    /// </summary>
    public bool OutboxEnabled { get; init; }

    /// <summary>
    /// Batch size for outbox processing.
    /// Default: 100. Override: LOADTEST_FEATURE_OUTBOX_BATCH_SIZE
    /// </summary>
    public int OutboxBatchSize { get; init; } = 100;

    /// <summary>
    /// Interval for outbox processing.
    /// Default: 1 second. Override: LOADTEST_FEATURE_OUTBOX_PROCESSING_INTERVAL
    /// </summary>
    public TimeSpan OutboxProcessingInterval { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Whether idempotency is enabled.
    /// Default: false. Override: LOADTEST_FEATURE_IDEMPOTENCY_ENABLED
    /// </summary>
    public bool IdempotencyEnabled { get; init; }

    /// <summary>
    /// Retention period for idempotency records.
    /// Default: 7 days. Override: LOADTEST_FEATURE_IDEMPOTENCY_RETENTION
    /// </summary>
    public TimeSpan IdempotencyRetentionPeriod { get; init; } = TimeSpan.FromDays(7);

    #endregion

    #region Resilience Features

    /// <summary>
    /// Whether retry is enabled.
    /// Default: false. Override: LOADTEST_FEATURE_RETRY_ENABLED
    /// </summary>
    public bool RetryEnabled { get; init; }

    /// <summary>
    /// Maximum number of retry attempts.
    /// Default: 3. Override: LOADTEST_FEATURE_RETRY_MAX_ATTEMPTS
    /// </summary>
    public int RetryMaxAttempts { get; init; } = 3;

    /// <summary>
    /// Initial delay between retries.
    /// Default: 100ms. Override: LOADTEST_FEATURE_RETRY_INITIAL_DELAY
    /// </summary>
    public TimeSpan RetryInitialDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Whether circuit breaker is enabled.
    /// Default: false. Override: LOADTEST_FEATURE_CIRCUIT_BREAKER_ENABLED
    /// </summary>
    public bool CircuitBreakerEnabled { get; init; }

    /// <summary>
    /// Number of failures before circuit opens.
    /// Default: 5. Override: LOADTEST_FEATURE_CIRCUIT_BREAKER_THRESHOLD
    /// </summary>
    public int CircuitBreakerFailureThreshold { get; init; } = 5;

    /// <summary>
    /// Duration the circuit stays open.
    /// Default: 30 seconds. Override: LOADTEST_FEATURE_CIRCUIT_BREAKER_DURATION
    /// </summary>
    public TimeSpan CircuitBreakerDurationOfBreak { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Whether timeout is enabled.
    /// Default: false. Override: LOADTEST_FEATURE_TIMEOUT_ENABLED
    /// </summary>
    public bool TimeoutEnabled { get; init; }

    /// <summary>
    /// Timeout duration for message processing.
    /// Default: 30 seconds. Override: LOADTEST_FEATURE_TIMEOUT_DURATION
    /// </summary>
    public TimeSpan TimeoutDuration { get; init; } = TimeSpan.FromSeconds(30);

    #endregion

    #region Computed Properties

    /// <summary>
    /// Whether SQL Server is required (outbox or idempotency enabled).
    /// </summary>
    public bool RequiresSqlServer => OutboxEnabled || IdempotencyEnabled;

    /// <summary>
    /// Whether any resilience features are enabled.
    /// </summary>
    public bool HasResilienceFeatures => RetryEnabled || CircuitBreakerEnabled || TimeoutEnabled;

    /// <summary>
    /// Whether any feature is enabled.
    /// </summary>
    public bool HasAnyFeature => RequiresSqlServer || HasResilienceFeatures;

    #endregion

    /// <summary>
    /// Creates configuration with no features enabled (baseline).
    /// </summary>
    public static FeatureConfiguration None => new();

    /// <summary>
    /// Creates configuration with all features enabled.
    /// </summary>
    public static FeatureConfiguration All => new()
    {
        OutboxEnabled = true,
        IdempotencyEnabled = true,
        RetryEnabled = true,
        CircuitBreakerEnabled = true,
        TimeoutEnabled = true
    };

    /// <summary>
    /// Creates configuration from environment variables.
    /// </summary>
    public static FeatureConfiguration FromEnvironment()
    {
        return new FeatureConfiguration
        {
            // Persistence
            OutboxEnabled = GetEnvBool("LOADTEST_FEATURE_OUTBOX_ENABLED", false),
            OutboxBatchSize = GetEnvInt("LOADTEST_FEATURE_OUTBOX_BATCH_SIZE", 100),
            OutboxProcessingInterval = GetEnvTimeSpan("LOADTEST_FEATURE_OUTBOX_PROCESSING_INTERVAL", TimeSpan.FromSeconds(1)),
            IdempotencyEnabled = GetEnvBool("LOADTEST_FEATURE_IDEMPOTENCY_ENABLED", false),
            IdempotencyRetentionPeriod = GetEnvTimeSpan("LOADTEST_FEATURE_IDEMPOTENCY_RETENTION", TimeSpan.FromDays(7)),

            // Resilience
            RetryEnabled = GetEnvBool("LOADTEST_FEATURE_RETRY_ENABLED", false),
            RetryMaxAttempts = GetEnvInt("LOADTEST_FEATURE_RETRY_MAX_ATTEMPTS", 3),
            RetryInitialDelay = GetEnvTimeSpan("LOADTEST_FEATURE_RETRY_INITIAL_DELAY", TimeSpan.FromMilliseconds(100)),
            CircuitBreakerEnabled = GetEnvBool("LOADTEST_FEATURE_CIRCUIT_BREAKER_ENABLED", false),
            CircuitBreakerFailureThreshold = GetEnvInt("LOADTEST_FEATURE_CIRCUIT_BREAKER_THRESHOLD", 5),
            CircuitBreakerDurationOfBreak = GetEnvTimeSpan("LOADTEST_FEATURE_CIRCUIT_BREAKER_DURATION", TimeSpan.FromSeconds(30)),
            TimeoutEnabled = GetEnvBool("LOADTEST_FEATURE_TIMEOUT_ENABLED", false),
            TimeoutDuration = GetEnvTimeSpan("LOADTEST_FEATURE_TIMEOUT_DURATION", TimeSpan.FromSeconds(30))
        };
    }

    private static bool GetEnvBool(string name, bool defaultValue)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return bool.TryParse(value, out var result) ? result : defaultValue;
    }

    private static int GetEnvInt(string name, int defaultValue)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return int.TryParse(value, out var result) ? result : defaultValue;
    }

    private static TimeSpan GetEnvTimeSpan(string name, TimeSpan defaultValue)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return TimeSpan.TryParse(value, out var result) ? result : defaultValue;
    }

    /// <summary>
    /// Returns a description of enabled features.
    /// </summary>
    public override string ToString()
    {
        var features = new List<string>();
        if (OutboxEnabled) features.Add("Outbox");
        if (IdempotencyEnabled) features.Add("Idempotency");
        if (RetryEnabled) features.Add("Retry");
        if (CircuitBreakerEnabled) features.Add("CircuitBreaker");
        if (TimeoutEnabled) features.Add("Timeout");
        return features.Count == 0 ? "None" : string.Join(", ", features);
    }
}

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

    #region Feature Configuration

    /// <summary>
    /// Feature configuration for middleware testing.
    /// </summary>
    public FeatureConfiguration Features { get; init; } = FeatureConfiguration.None;

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
            MetricsReportingInterval = GetEnvTimeSpan("LOADTEST_REPORTING_INTERVAL", TimeSpan.FromSeconds(30)),

            // Features
            Features = FeatureConfiguration.FromEnvironment()
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

    private static bool GetEnvBool(string name, bool defaultValue)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return bool.TryParse(value, out var result) ? result : defaultValue;
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
