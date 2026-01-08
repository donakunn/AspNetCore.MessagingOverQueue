namespace Donakunn.MessagingOverQueue.Persistence.Entities;

/// <summary>
/// Entity representing a processed message in the inbox (for idempotency).
/// </summary>
public class InboxMessage
{
    /// <summary>
    /// The message ID.
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>
    /// The message type.
    /// </summary>
    public string MessageType { get; set; } = string.Empty;

    /// <summary>
    /// The handler type that processed this message.
    /// </summary>
    public string HandlerType { get; set; } = string.Empty;

    /// <summary>
    /// When the message was processed.
    /// </summary>
    public DateTime ProcessedAt { get; set; }

    /// <summary>
    /// Correlation ID for tracking.
    /// </summary>
    public string? CorrelationId { get; set; }
}

