namespace MessagingOverQueue.src.Abstractions.Messages;

/// <summary>
/// Base interface for all messages in the system.
/// </summary>
public interface IMessage
{
    /// <summary>
    /// Unique identifier for the message.
    /// </summary>
    Guid Id { get; }

    /// <summary>
    /// Timestamp when the message was created.
    /// </summary>
    DateTime Timestamp { get; }

    /// <summary>
    /// Correlation ID for tracking related messages across services.
    /// </summary>
    string? CorrelationId { get; }

    /// <summary>
    /// Causation ID linking this message to its cause.
    /// </summary>
    string? CausationId { get; }

    /// <summary>
    /// The type name of the message for deserialization.
    /// </summary>
    string MessageType { get; }
}

