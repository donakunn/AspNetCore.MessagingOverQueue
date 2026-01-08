using Donakunn.MessagingOverQueue.Abstractions.Messages;

namespace Donakunn.MessagingOverQueue.Publishing.Middleware;

/// <summary>
/// Middleware for the publish pipeline.
/// </summary>
public interface IPublishMiddleware
{
    /// <summary>
    /// Processes the message through this middleware.
    /// </summary>
    /// <param name="context">The publish context.</param>
    /// <param name="next">The next middleware in the pipeline.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task InvokeAsync(PublishContext context, Func<PublishContext, CancellationToken, Task> next, CancellationToken cancellationToken);
}

/// <summary>
/// Context for publishing a message.
/// </summary>
public class PublishContext
{
    /// <summary>
    /// The message being published. May be null for raw publishing (e.g., from OutboxProcessor).
    /// </summary>
    public IMessage? Message { get; init; }

    /// <summary>
    /// The message type. May be null for raw publishing.
    /// </summary>
    public Type? MessageType { get; init; }

    /// <summary>
    /// The serialized message body.
    /// </summary>
    public byte[]? Body { get; set; }

    /// <summary>
    /// The exchange name.
    /// </summary>
    public string? ExchangeName { get; set; }

    /// <summary>
    /// The routing key.
    /// </summary>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Whether the message is persistent.
    /// </summary>
    public bool Persistent { get; set; } = true;

    /// <summary>
    /// Message priority.
    /// </summary>
    public byte? Priority { get; set; }

    /// <summary>
    /// Message TTL.
    /// </summary>
    public int? TimeToLive { get; set; }

    /// <summary>
    /// Message headers.
    /// </summary>
    public Dictionary<string, object?> Headers { get; set; } = new();

    /// <summary>
    /// Content type.
    /// </summary>
    public string ContentType { get; set; } = "application/json";

    /// <summary>
    /// Whether to wait for publisher confirms.
    /// </summary>
    public bool WaitForConfirm { get; set; } = true;

    /// <summary>
    /// Confirm timeout.
    /// </summary>
    public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Custom data storage.
    /// </summary>
    public Dictionary<string, object> Data { get; } = new();
}

