namespace Donakunn.MessagingOverQueue.Configuration.Options;

/// <summary>
/// Configuration options for the outbox pattern.
/// </summary>
public class OutboxOptions
{
    /// <summary>
    /// Configuration section name.
    /// </summary>
    public const string SectionName = "RabbitMq:Outbox";

    /// <summary>
    /// Whether the outbox is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Interval for processing pending outbox messages.
    /// </summary>
    public TimeSpan ProcessingInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Batch size for processing outbox messages.
    /// </summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// Maximum age of messages before they are considered stale.
    /// </summary>
    public TimeSpan MaxMessageAge { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Whether to automatically cleanup processed messages.
    /// </summary>
    public bool AutoCleanup { get; set; } = true;

    /// <summary>
    /// Retention period for processed messages.
    /// </summary>
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(1);

    /// <summary>
    /// Lock duration for processing messages.
    /// </summary>
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum retry attempts for outbox messages.
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 5;
}

