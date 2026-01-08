namespace Donakunn.MessagingOverQueue.Topology.Attributes;

/// <summary>
/// Configures message retry behavior.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class RetryPolicyAttribute : Attribute
{
    /// <summary>
    /// Maximum number of retry attempts. Defaults to 3.
    /// </summary>
    public int MaxRetries { get; init; } = 3;

    /// <summary>
    /// Initial delay between retries in milliseconds. Defaults to 1000.
    /// </summary>
    public int InitialDelayMs { get; init; } = 1000;

    /// <summary>
    /// Maximum delay between retries in milliseconds. Defaults to 30000.
    /// </summary>
    public int MaxDelayMs { get; init; } = 30000;

    /// <summary>
    /// Whether to use exponential backoff. Defaults to true.
    /// </summary>
    public bool UseExponentialBackoff { get; init; } = true;

    /// <summary>
    /// Creates a new retry policy attribute with default settings.
    /// </summary>
    public RetryPolicyAttribute() { }

    /// <summary>
    /// Creates a new retry policy attribute with the specified max retries.
    /// </summary>
    /// <param name="maxRetries">Maximum number of retry attempts.</param>
    public RetryPolicyAttribute(int maxRetries)
    {
        MaxRetries = maxRetries;
    }
}
