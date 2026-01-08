using Donakunn.MessagingOverQueue.Persistence.Entities;

namespace Donakunn.MessagingOverQueue.Persistence.Repositories;

/// <summary>
/// Repository interface for outbox operations.
/// </summary>
public interface IOutboxRepository
{
    /// <summary>
    /// Adds a message to the outbox.
    /// </summary>
    Task AddAsync(OutboxMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets pending messages that are ready for processing.
    /// </summary>
    Task<IReadOnlyList<OutboxMessage>> GetPendingMessagesAsync(int batchSize, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acquires a lock on messages for processing.
    /// </summary>
    Task<IReadOnlyList<OutboxMessage>> AcquireLockAsync(int batchSize, TimeSpan lockDuration, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks a message as published.
    /// </summary>
    Task MarkAsPublishedAsync(Guid messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks a message as failed.
    /// </summary>
    Task MarkAsFailedAsync(Guid messageId, string error, CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases the lock on a message.
    /// </summary>
    Task ReleaseLockAsync(Guid messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Cleans up old processed messages.
    /// </summary>
    Task CleanupAsync(TimeSpan retentionPeriod, CancellationToken cancellationToken = default);

    /// <summary>
    /// Saves changes.
    /// </summary>
    Task SaveChangesAsync(CancellationToken cancellationToken = default);
}

