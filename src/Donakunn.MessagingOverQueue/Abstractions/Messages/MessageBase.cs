namespace MessagingOverQueue.src.Abstractions.Messages;

/// <summary>
/// Abstract base class for all messages providing common functionality.
/// </summary>
public abstract class MessageBase : IMessage
{
    /// <inheritdoc />
    public Guid Id { get; init; } = Guid.NewGuid();

    /// <inheritdoc />
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;

    /// <inheritdoc />
    public string? CorrelationId { get; init; }

    /// <inheritdoc />
    public string? CausationId { get; init; }

    /// <inheritdoc />
    public virtual string MessageType => GetType().AssemblyQualifiedName ?? GetType().FullName ?? GetType().Name;

    /// <summary>
    /// Creates a new message with the specified correlation ID.
    /// </summary>
    public T WithCorrelationId<T>(string correlationId) where T : MessageBase
    {
        var clone = (T)MemberwiseClone();
        typeof(MessageBase).GetProperty(nameof(CorrelationId))!.SetValue(clone, correlationId);
        return clone;
    }
}

