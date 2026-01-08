using Donakunn.MessagingOverQueue.Abstractions.Consuming;
using Donakunn.MessagingOverQueue.Abstractions.Messages;

namespace Donakunn.MessagingOverQueue.Consuming.Middleware;

/// <summary>
/// Middleware for the consume pipeline.
/// </summary>
public interface IConsumeMiddleware
{
    /// <summary>
    /// Processes the message through this middleware.
    /// </summary>
    /// <param name="context">The consume context.</param>
    /// <param name="next">The next middleware in the pipeline.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken);
}

/// <summary>
/// Context for consuming a message.
/// </summary>
public class ConsumeContext
{
    /// <summary>
    /// The raw message body.
    /// </summary>
    public byte[] Body { get; init; } = null!;

    /// <summary>
    /// The deserialized message.
    /// </summary>
    public IMessage? Message { get; set; }

    /// <summary>
    /// The message type.
    /// </summary>
    public Type? MessageType { get; set; }

    /// <summary>
    /// The message context.
    /// </summary>
    public IMessageContext MessageContext { get; init; } = null!;

    /// <summary>
    /// The delivery tag for acknowledgement.
    /// </summary>
    public ulong DeliveryTag { get; init; }

    /// <summary>
    /// Whether the message was redelivered.
    /// </summary>
    public bool Redelivered { get; init; }

    /// <summary>
    /// Headers from the message properties.
    /// </summary>
    public IReadOnlyDictionary<string, object> Headers { get; init; } = new Dictionary<string, object>();

    /// <summary>
    /// Content type.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// Whether the message should be acknowledged.
    /// </summary>
    public bool ShouldAck { get; set; } = true;

    /// <summary>
    /// Whether the message should be rejected.
    /// </summary>
    public bool ShouldReject { get; set; }

    /// <summary>
    /// Whether to requeue on rejection.
    /// </summary>
    public bool RequeueOnReject { get; set; } = true;

    /// <summary>
    /// Exception that occurred during processing.
    /// </summary>
    public Exception? Exception { get; set; }

    /// <summary>
    /// Custom data storage.
    /// </summary>
    public Dictionary<string, object> Data { get; } = new();
}

