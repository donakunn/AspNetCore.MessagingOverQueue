namespace MessagingOverQueue.src.Configuration.Options;

/// <summary>
/// Configuration options for retry policies.
/// </summary>
public class RetryOptions
{
    /// <summary>
    /// Configuration section name.
    /// </summary>
    public const string SectionName = "RabbitMq:Retry";

    /// <summary>
    /// Maximum number of retry attempts.
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>
    /// Initial delay before first retry.
    /// </summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Maximum delay between retries.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Delay multiplier for exponential backoff.
    /// </summary>
    public double DelayMultiplier { get; set; } = 2.0;

    /// <summary>
    /// Whether to use exponential backoff.
    /// </summary>
    public bool UseExponentialBackoff { get; set; } = true;

    /// <summary>
    /// Whether to add jitter to delays.
    /// </summary>
    public bool AddJitter { get; set; } = true;

    /// <summary>
    /// Maximum jitter as a percentage of delay.
    /// </summary>
    public double JitterFactor { get; set; } = 0.2;

    /// <summary>
    /// Exception types that should not trigger a retry.
    /// </summary>
    public List<string> NonRetryableExceptions { get; set; } = new();
}

