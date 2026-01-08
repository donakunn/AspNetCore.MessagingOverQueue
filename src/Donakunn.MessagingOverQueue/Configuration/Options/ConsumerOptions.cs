namespace Donakunn.MessagingOverQueue.Configuration.Options;

/// <summary>
/// Configuration options for message consumers.
/// </summary>
public class ConsumerOptions
{
    /// <summary>
    /// Queue name to consume from.
    /// </summary>
    public string QueueName { get; set; } = string.Empty;

    /// <summary>
    /// Number of messages to prefetch.
    /// </summary>
    public ushort PrefetchCount { get; set; } = 10;

    /// <summary>
    /// Whether to auto-acknowledge messages.
    /// </summary>
    public bool AutoAck { get; set; }

    /// <summary>
    /// Consumer tag.
    /// </summary>
    public string? ConsumerTag { get; set; }

    /// <summary>
    /// Whether to requeue failed messages.
    /// </summary>
    public bool RequeueOnFailure { get; set; } = true;

    /// <summary>
    /// Maximum number of concurrent handlers.
    /// </summary>
    public int MaxConcurrency { get; set; } = 1;

    /// <summary>
    /// Timeout for message processing.
    /// </summary>
    public TimeSpan ProcessingTimeout { get; set; } = TimeSpan.FromMinutes(5);
}

