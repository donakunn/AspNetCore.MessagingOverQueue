using MessagingOverQueue.src.Abstractions.Messages;

namespace MessagingOverQueue.src.Persistence.Repositories;

/// <summary>
/// Repository interface for inbox operations (idempotency).
/// </summary>
public interface IInboxRepository
{
    /// <summary>
    /// Checks if a message has already been processed by a specific handler.
    /// </summary>
    Task<bool> HasBeenProcessedAsync(Guid messageId, string handlerType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks a message as processed by a specific handler.
    /// </summary>
    Task MarkAsProcessedAsync(IMessage message, string handlerType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Cleans up old inbox records.
    /// </summary>
    Task CleanupAsync(TimeSpan retentionPeriod, CancellationToken cancellationToken = default);
}

