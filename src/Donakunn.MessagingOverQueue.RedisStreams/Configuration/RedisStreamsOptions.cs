namespace Donakunn.MessagingOverQueue.RedisStreams.Configuration;

/// <summary>
/// Configuration options for Redis Streams messaging.
/// </summary>
public sealed class RedisStreamsOptions
{
    /// <summary>
    /// Configuration section name for binding from appsettings.json.
    /// </summary>
    public const string SectionName = "RedisStreams";

    /// <summary>
    /// Redis connection string (e.g., "localhost:6379" or "redis-server:6379,password=secret").
    /// </summary>
    public string ConnectionString { get; set; } = "localhost:6379";

    /// <summary>
    /// Optional password for Redis authentication.
    /// This is an alternative to including password in connection string.
    /// </summary>
    public string? Password { get; set; }

    /// <summary>
    /// Redis database index (0-15).
    /// </summary>
    public int Database { get; set; } = 0;

    /// <summary>
    /// Prefix for stream keys (e.g., "messaging" results in "messaging:events.order-created").
    /// </summary>
    public string StreamPrefix { get; set; } = "messaging";

    /// <summary>
    /// Connection timeout.
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Number of times to retry connecting.
    /// </summary>
    public int ConnectRetry { get; set; } = 3;

    /// <summary>
    /// Whether to abort if initial connection fails.
    /// Set to false for resilient startup.
    /// </summary>
    public bool AbortOnConnectFail { get; set; } = false;

    /// <summary>
    /// Client name for identification in Redis.
    /// </summary>
    public string? ClientName { get; set; }

    /// <summary>
    /// Sync timeout for Redis operations.
    /// </summary>
    public TimeSpan SyncTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Async timeout for Redis operations.
    /// </summary>
    public TimeSpan AsyncTimeout { get; set; } = TimeSpan.FromSeconds(5);

    // ===== Consumer Settings =====

    /// <summary>
    /// Time after which idle messages can be claimed by another consumer.
    /// </summary>
    public TimeSpan ClaimIdleTime { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum number of pending messages per consumer before backpressure is applied.
    /// </summary>
    public int MaxPendingMessages { get; set; } = 1000;

    /// <summary>
    /// Timeout for blocking read operations (XREADGROUP BLOCK).
    /// </summary>
    public TimeSpan BlockingTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Interval for checking and claiming idle messages from other consumers.
    /// </summary>
    public TimeSpan ClaimCheckInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Number of messages to read in each batch.
    /// </summary>
    public int BatchSize { get; set; } = 10;

    /// <summary>
    /// Unique consumer identifier. If not set, auto-generated from machine name.
    /// </summary>
    public string? ConsumerId { get; set; }

    // ===== Stream Retention Settings =====

    /// <summary>
    /// Strategy for retaining messages in streams.
    /// </summary>
    public StreamRetentionStrategy RetentionStrategy { get; set; } = StreamRetentionStrategy.TimeBased;

    /// <summary>
    /// Retention period for time-based strategy.
    /// Messages older than this will be trimmed.
    /// </summary>
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Maximum stream length for count-based strategy.
    /// </summary>
    public long? MaxStreamLength { get; set; }

    /// <summary>
    /// Use approximate trimming (~) for better performance.
    /// </summary>
    public bool ApproximateTrimming { get; set; } = true;

    // ===== Dead Letter Settings =====

    /// <summary>
    /// Strategy for handling messages that exceed max delivery attempts.
    /// </summary>
    public DeadLetterStrategy DeadLetterStrategy { get; set; } = DeadLetterStrategy.PerConsumerGroup;

    /// <summary>
    /// Maximum number of delivery attempts before moving to dead letter.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 5;

    /// <summary>
    /// Suffix for dead letter queue stream names.
    /// </summary>
    public string DeadLetterSuffix { get; set; } = "dlq";

    // ===== SSL/TLS Settings =====

    /// <summary>
    /// Whether to use SSL/TLS for the connection.
    /// </summary>
    public bool UseSsl { get; set; }

    /// <summary>
    /// SSL host name for certificate validation.
    /// </summary>
    public string? SslHost { get; set; }
}

/// <summary>
/// Strategy for retaining messages in Redis Streams.
/// </summary>
public enum StreamRetentionStrategy
{
    /// <summary>
    /// No automatic trimming.
    /// </summary>
    None,

    /// <summary>
    /// Trim messages older than RetentionPeriod using MINID.
    /// </summary>
    TimeBased,

    /// <summary>
    /// Trim to MaxStreamLength using MAXLEN.
    /// </summary>
    CountBased
}

/// <summary>
/// Strategy for handling messages that exceed max delivery attempts.
/// </summary>
public enum DeadLetterStrategy
{
    /// <summary>
    /// No dead letter handling - messages are dropped after max attempts.
    /// </summary>
    Disabled,

    /// <summary>
    /// Single DLQ stream per source stream: {stream}:dlq
    /// </summary>
    PerStream,

    /// <summary>
    /// Separate DLQ per consumer group: {stream}:{group}:dlq
    /// </summary>
    PerConsumerGroup
}
