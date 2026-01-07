namespace MessagingOverQueue.src.Persistence.Entities;

/// <summary>
/// Entity representing a message in the outbox.
/// </summary>
public class OutboxMessage
{
    /// <summary>
    /// Unique identifier.
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>
    /// The message type (assembly qualified name).
    /// </summary>
    public string MessageType { get; set; } = string.Empty;

    /// <summary>
    /// The serialized message payload.
    /// </summary>
    public byte[] Payload { get; set; } = Array.Empty<byte>();

    /// <summary>
    /// The target exchange.
    /// </summary>
    public string? ExchangeName { get; set; }

    /// <summary>
    /// The routing key.
    /// </summary>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Serialized headers as JSON.
    /// </summary>
    public string? Headers { get; set; }

    /// <summary>
    /// When the message was created.
    /// </summary>
    public DateTime CreatedAt { get; set; }

    /// <summary>
    /// When the message was last processed.
    /// </summary>
    public DateTime? ProcessedAt { get; set; }

    /// <summary>
    /// Status of the message.
    /// </summary>
    public OutboxMessageStatus Status { get; set; }

    /// <summary>
    /// Number of processing attempts.
    /// </summary>
    public int RetryCount { get; set; }

    /// <summary>
    /// Last error message if processing failed.
    /// </summary>
    public string? LastError { get; set; }

    /// <summary>
    /// Lock token for distributed processing.
    /// </summary>
    public string? LockToken { get; set; }

    /// <summary>
    /// When the lock expires.
    /// </summary>
    public DateTime? LockExpiresAt { get; set; }

    /// <summary>
    /// Correlation ID for tracking.
    /// </summary>
    public string? CorrelationId { get; set; }
}

/// <summary>
/// Status of an outbox message.
/// </summary>
public enum OutboxMessageStatus
{
    /// <summary>
    /// Message is pending processing.
    /// </summary>
    Pending = 0,

    /// <summary>
    /// Message is being processed.
    /// </summary>
    Processing = 1,

    /// <summary>
    /// Message was successfully published.
    /// </summary>
    Published = 2,

    /// <summary>
    /// Message processing failed.
    /// </summary>
    Failed = 3
}

