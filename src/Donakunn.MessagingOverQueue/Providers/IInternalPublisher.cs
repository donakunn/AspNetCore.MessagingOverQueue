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
}
