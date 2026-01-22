using Donakunn.MessagingOverQueue.Publishing.Middleware;

namespace Donakunn.MessagingOverQueue.Providers;

/// <summary>
/// Internal publisher abstraction for sending messages directly to a broker.
/// This interface is implemented by provider-specific publishers (RabbitMQ, Redis Streams, etc.)
/// and is used by the middleware pipeline for the final publish step.
/// </summary>
public interface IInternalPublisher
{
    /// <summary>
    /// Publishes a message to the broker.
    /// </summary>
    /// <param name="context">The publish context containing message data and routing information.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <remarks>
    /// The context may contain:
    /// - Message: The typed message object (may be null for raw publishing)
    /// - Body: The serialized message bytes
    /// - ExchangeName/RoutingKey: Routing information
    /// - Headers: Message metadata
    /// </remarks>
    Task PublishAsync(PublishContext context, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes multiple messages to the broker in a single batch operation.
    /// Uses provider-specific batching (e.g., Redis pipelining) for better throughput.
    /// </summary>
    /// <param name="contexts">The publish contexts to send.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Results for each publish operation, allowing partial success handling.</returns>
    Task<IReadOnlyList<PublishResult>> PublishBatchAsync(
        IReadOnlyList<PublishContext> contexts,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Result of a single publish operation within a batch.
/// </summary>
public sealed class PublishResult
{
    /// <summary>
    /// The message ID that was published.
    /// </summary>
    public Guid MessageId { get; }

    /// <summary>
    /// Whether the publish was successful.
    /// </summary>
    public bool Success { get; }

    /// <summary>
    /// Error message if the publish failed.
    /// </summary>
    public string? Error { get; }

    private PublishResult(Guid messageId, bool success, string? error)
    {
        MessageId = messageId;
        Success = success;
        Error = error;
    }

    /// <summary>
    /// Creates a successful publish result.
    /// </summary>
    public static PublishResult Succeeded(Guid messageId) => new(messageId, true, null);

    /// <summary>
    /// Creates a failed publish result.
    /// </summary>
    public static PublishResult Failed(Guid messageId, string error) => new(messageId, false, error);
}
