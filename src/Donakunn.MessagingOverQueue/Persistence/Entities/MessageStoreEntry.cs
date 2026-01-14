namespace Donakunn.MessagingOverQueue.Persistence.Entities;

/// <summary>
/// Unified entity representing a message in the message store.
/// Used for both outbox (outgoing messages) and inbox (processed messages for idempotency).
/// </summary>
public sealed class MessageStoreEntry
{
    /// <summary>
    /// Unique identifier for the message.
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>
    /// Direction of the message (Outbox for publishing, Inbox for consumption idempotency).
    /// </summary>
    public MessageDirection Direction { get; set; }

    /// <summary>
    /// The message type (fully qualified type name).
    /// </summary>
    public string MessageType { get; set; } = string.Empty;

    /// <summary>
    /// The serialized message payload (null for inbox entries).
    /// </summary>
    public byte[]? Payload { get; set; }

    /// <summary>
    /// The target exchange (outbox only).
    /// </summary>
    public string? ExchangeName { get; set; }

    /// <summary>
    /// The routing key (outbox only).
    /// </summary>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Serialized headers as JSON (outbox only).
    /// </summary>
    public string? Headers { get; set; }

    /// <summary>
    /// The handler type that processed this message (inbox only, for per-handler idempotency).
    /// </summary>
    public string? HandlerType { get; set; }

    /// <summary>
    /// When the entry was created.
    /// </summary>
    public DateTime CreatedAt { get; set; }

    /// <summary>
    /// When the message was processed/published.
    /// </summary>
    public DateTime? ProcessedAt { get; set; }

    /// <summary>
    /// Status of the message (outbox only).
    /// </summary>
    public MessageStatus Status { get; set; }

    /// <summary>
    /// Number of processing attempts (outbox only).
    /// </summary>
    public int RetryCount { get; set; }

    /// <summary>
    /// Last error message if processing failed (outbox only).
    /// </summary>
    public string? LastError { get; set; }

    /// <summary>
    /// Lock token for distributed processing (outbox only).
    /// </summary>
    public string? LockToken { get; set; }

    /// <summary>
    /// When the lock expires (outbox only).
    /// </summary>
    public DateTime? LockExpiresAt { get; set; }

    /// <summary>
    /// Correlation ID for tracking.
    /// </summary>
    public string? CorrelationId { get; set; }

    /// <summary>
    /// Creates an outbox entry for a message to be published.
    /// </summary>
    public static MessageStoreEntry CreateOutboxEntry(
        Guid id,
        string messageType,
        byte[] payload,
        string? exchangeName,
        string? routingKey,
        string? headers,
        string? correlationId)
    {
        return new MessageStoreEntry
        {
            Id = id,
            Direction = MessageDirection.Outbox,
            MessageType = messageType,
            Payload = payload,
            ExchangeName = exchangeName,
            RoutingKey = routingKey,
            Headers = headers,
            HandlerType = string.Empty, // Required for NOT NULL constraint
            CorrelationId = correlationId,
            CreatedAt = DateTime.UtcNow,
            Status = MessageStatus.Pending,
            RetryCount = 0
        };
    }

    /// <summary>
    /// Creates an inbox entry for a processed message (idempotency tracking).
    /// </summary>
    public static MessageStoreEntry CreateInboxEntry(
        Guid messageId,
        string messageType,
        string handlerType,
        string? correlationId)
    {
        return new MessageStoreEntry
        {
            Id = messageId,
            Direction = MessageDirection.Inbox,
            MessageType = messageType,
            HandlerType = handlerType,
            CorrelationId = correlationId,
            CreatedAt = DateTime.UtcNow,
            ProcessedAt = DateTime.UtcNow,
            Status = MessageStatus.Published // Inbox entries are always "completed"
        };
    }
}

/// <summary>
/// Direction of the message in the store.
/// </summary>
public enum MessageDirection
{
    /// <summary>
    /// Outgoing message to be published.
    /// </summary>
    Outbox = 0,

    /// <summary>
    /// Incoming message processed (for idempotency).
    /// </summary>
    Inbox = 1
}

/// <summary>
/// Status of a message in the store.
/// </summary>
public enum MessageStatus
{
    /// <summary>
    /// Message is pending processing.
    /// </summary>
    Pending = 0,

    /// <summary>
    /// Message is being processed (locked).
    /// </summary>
    Processing = 1,

    /// <summary>
    /// Message was successfully published/processed.
    /// </summary>
    Published = 2,

    /// <summary>
    /// Message processing failed.
    /// </summary>
    Failed = 3
}
