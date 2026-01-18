namespace Donakunn.MessagingOverQueue.Consuming.Middleware;

/// <summary>
/// Defines execution order constants for consume middleware.
/// Lower values execute first (outer middleware), higher values execute later (inner middleware).
/// </summary>
public static class MiddlewareOrder
{
    /// <summary>
    /// Circuit breaker - fail fast when circuit is open.
    /// Executes first to prevent unnecessary work.
    /// </summary>
    public const int CircuitBreaker = 100;

    /// <summary>
    /// Retry - wraps processing with retry logic.
    /// Executes after circuit breaker to retry on transient failures.
    /// </summary>
    public const int Retry = 200;

    /// <summary>
    /// Timeout - enforces maximum processing time.
    /// Executes after retry to timeout individual attempts.
    /// </summary>
    public const int Timeout = 300;

    /// <summary>
    /// Logging - logs entry/exit and timing.
    /// Executes after resilience middleware to log actual processing.
    /// </summary>
    public const int Logging = 400;

    /// <summary>
    /// Idempotency - checks for duplicate messages.
    /// Executes before deserialization to skip duplicates early.
    /// </summary>
    public const int Idempotency = 500;

    /// <summary>
    /// Deserialization - deserializes the message body.
    /// Executes last before handler invocation.
    /// </summary>
    public const int Deserialization = 600;

    /// <summary>
    /// Default order for middleware that doesn't specify an order.
    /// Placed after deserialization.
    /// </summary>
    public const int Default = 1000;
}
