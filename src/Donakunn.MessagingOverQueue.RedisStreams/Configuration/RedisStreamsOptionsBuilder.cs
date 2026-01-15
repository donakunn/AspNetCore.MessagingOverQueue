namespace Donakunn.MessagingOverQueue.RedisStreams.Configuration;

/// <summary>
/// Fluent builder for configuring Redis Streams options.
/// </summary>
public sealed class RedisStreamsOptionsBuilder
{
    private readonly RedisStreamsOptions _options = new();

    /// <summary>
    /// Sets the Redis connection string.
    /// </summary>
    /// <param name="connectionString">Connection string (e.g., "localhost:6379").</param>
    public RedisStreamsOptionsBuilder UseConnectionString(string connectionString)
    {
        _options.ConnectionString = connectionString;
        return this;
    }

    /// <summary>
    /// Sets the Redis password.
    /// </summary>
    /// <param name="password">The password for Redis authentication.</param>
    public RedisStreamsOptionsBuilder WithPassword(string password)
    {
        _options.Password = password;
        return this;
    }

    /// <summary>
    /// Sets the Redis database index.
    /// </summary>
    /// <param name="database">Database index (0-15).</param>
    public RedisStreamsOptionsBuilder UseDatabase(int database)
    {
        _options.Database = database;
        return this;
    }

    /// <summary>
    /// Sets the stream key prefix.
    /// </summary>
    /// <param name="prefix">Prefix for all stream keys.</param>
    public RedisStreamsOptionsBuilder WithStreamPrefix(string prefix)
    {
        _options.StreamPrefix = prefix;
        return this;
    }

    /// <summary>
    /// Sets the client name for Redis identification.
    /// </summary>
    /// <param name="clientName">Client name.</param>
    public RedisStreamsOptionsBuilder WithClientName(string clientName)
    {
        _options.ClientName = clientName;
        return this;
    }

    /// <summary>
    /// Configures connection timeout settings.
    /// </summary>
    /// <param name="connectTimeout">Connection timeout.</param>
    /// <param name="connectRetry">Number of connection retries.</param>
    public RedisStreamsOptionsBuilder WithConnectionTimeout(TimeSpan connectTimeout, int connectRetry = 3)
    {
        _options.ConnectTimeout = connectTimeout;
        _options.ConnectRetry = connectRetry;
        return this;
    }

    /// <summary>
    /// Sets whether to abort on connection failure.
    /// </summary>
    /// <param name="abort">True to abort, false for resilient startup.</param>
    public RedisStreamsOptionsBuilder AbortOnConnectFail(bool abort = true)
    {
        _options.AbortOnConnectFail = abort;
        return this;
    }

    /// <summary>
    /// Configures consumer settings. Only explicitly provided values are updated,
    /// allowing multiple calls to be composed without overwriting previous settings.
    /// </summary>
    /// <param name="batchSize">Messages per batch (default: 10).</param>
    /// <param name="blockingTimeout">Polling interval when no messages are available (default: 5 seconds).</param>
    /// <param name="maxPendingMessages">Max pending messages before backpressure (default: 1000).</param>
    public RedisStreamsOptionsBuilder ConfigureConsumer(
        int? batchSize = null,
        TimeSpan? blockingTimeout = null,
        int? maxPendingMessages = null)
    {
        if (batchSize.HasValue)
            _options.BatchSize = batchSize.Value;
        if (blockingTimeout.HasValue)
            _options.BlockingTimeout = blockingTimeout.Value;
        if (maxPendingMessages.HasValue)
            _options.MaxPendingMessages = maxPendingMessages.Value;
        return this;
    }

    /// <summary>
    /// Sets the consumer identifier.
    /// </summary>
    /// <param name="consumerId">Unique consumer ID.</param>
    public RedisStreamsOptionsBuilder WithConsumerId(string consumerId)
    {
        _options.ConsumerId = consumerId;
        return this;
    }

    /// <summary>
    /// Configures message claiming for failed consumers. Only explicitly provided values are updated,
    /// allowing multiple calls to be composed without overwriting previous settings.
    /// </summary>
    /// <param name="claimIdleTime">Time before message can be claimed by another consumer.</param>
    /// <param name="checkInterval">Interval between claim checks (default: 30 seconds).</param>
    public RedisStreamsOptionsBuilder ConfigureClaiming(
        TimeSpan? claimIdleTime = null,
        TimeSpan? checkInterval = null)
    {
        if (claimIdleTime.HasValue)
            _options.ClaimIdleTime = claimIdleTime.Value;
        if (checkInterval.HasValue)
            _options.ClaimCheckInterval = checkInterval.Value;
        return this;
    }

    /// <summary>
    /// Configures time-based stream retention.
    /// </summary>
    /// <param name="retentionPeriod">How long to retain messages.</param>
    /// <param name="approximateTrimming">Use approximate trimming for performance.</param>
    public RedisStreamsOptionsBuilder WithTimeBasedRetention(
        TimeSpan retentionPeriod,
        bool approximateTrimming = true)
    {
        _options.RetentionStrategy = StreamRetentionStrategy.TimeBased;
        _options.RetentionPeriod = retentionPeriod;
        _options.ApproximateTrimming = approximateTrimming;
        return this;
    }

    /// <summary>
    /// Configures count-based stream retention.
    /// </summary>
    /// <param name="maxLength">Maximum number of messages in stream.</param>
    /// <param name="approximateTrimming">Use approximate trimming for performance.</param>
    public RedisStreamsOptionsBuilder WithCountBasedRetention(
        long maxLength,
        bool approximateTrimming = true)
    {
        _options.RetentionStrategy = StreamRetentionStrategy.CountBased;
        _options.MaxStreamLength = maxLength;
        _options.ApproximateTrimming = approximateTrimming;
        return this;
    }

    /// <summary>
    /// Disables stream retention (no automatic trimming).
    /// </summary>
    public RedisStreamsOptionsBuilder WithNoRetention()
    {
        _options.RetentionStrategy = StreamRetentionStrategy.None;
        return this;
    }

    /// <summary>
    /// Configures dead letter handling per consumer group.
    /// </summary>
    /// <param name="maxDeliveryAttempts">Max attempts before DLQ.</param>
    public RedisStreamsOptionsBuilder WithDeadLetterPerConsumerGroup(int maxDeliveryAttempts = 5)
    {
        _options.DeadLetterStrategy = DeadLetterStrategy.PerConsumerGroup;
        _options.MaxDeliveryAttempts = maxDeliveryAttempts;
        return this;
    }

    /// <summary>
    /// Configures dead letter handling per stream.
    /// </summary>
    /// <param name="maxDeliveryAttempts">Max attempts before DLQ.</param>
    public RedisStreamsOptionsBuilder WithDeadLetterPerStream(int maxDeliveryAttempts = 5)
    {
        _options.DeadLetterStrategy = DeadLetterStrategy.PerStream;
        _options.MaxDeliveryAttempts = maxDeliveryAttempts;
        return this;
    }

    /// <summary>
    /// Disables dead letter handling.
    /// </summary>
    public RedisStreamsOptionsBuilder DisableDeadLetter()
    {
        _options.DeadLetterStrategy = DeadLetterStrategy.Disabled;
        return this;
    }

    /// <summary>
    /// Enables SSL/TLS for the connection.
    /// </summary>
    /// <param name="sslHost">SSL host name for certificate validation.</param>
    public RedisStreamsOptionsBuilder UseSsl(string? sslHost = null)
    {
        _options.UseSsl = true;
        _options.SslHost = sslHost;
        return this;
    }

    /// <summary>
    /// Builds the options instance.
    /// </summary>
    public RedisStreamsOptions Build() => _options;

    /// <summary>
    /// Configures an existing options instance.
    /// </summary>
    internal void Configure(RedisStreamsOptions options)
    {
        options.ConnectionString = _options.ConnectionString;
        options.Password = _options.Password;
        options.Database = _options.Database;
        options.StreamPrefix = _options.StreamPrefix;
        options.ConnectTimeout = _options.ConnectTimeout;
        options.ConnectRetry = _options.ConnectRetry;
        options.AbortOnConnectFail = _options.AbortOnConnectFail;
        options.ClientName = _options.ClientName;
        options.SyncTimeout = _options.SyncTimeout;
        options.AsyncTimeout = _options.AsyncTimeout;
        options.ClaimIdleTime = _options.ClaimIdleTime;
        options.MaxPendingMessages = _options.MaxPendingMessages;
        options.BlockingTimeout = _options.BlockingTimeout;
        options.ClaimCheckInterval = _options.ClaimCheckInterval;
        options.BatchSize = _options.BatchSize;
        options.ConsumerId = _options.ConsumerId;
        options.RetentionStrategy = _options.RetentionStrategy;
        options.RetentionPeriod = _options.RetentionPeriod;
        options.MaxStreamLength = _options.MaxStreamLength;
        options.ApproximateTrimming = _options.ApproximateTrimming;
        options.DeadLetterStrategy = _options.DeadLetterStrategy;
        options.MaxDeliveryAttempts = _options.MaxDeliveryAttempts;
        options.DeadLetterSuffix = _options.DeadLetterSuffix;
        options.UseSsl = _options.UseSsl;
        options.SslHost = _options.SslHost;
    }
}
