using Donakunn.MessagingOverQueue.Persistence.Entities;
using Donakunn.MessagingOverQueue.Persistence.Providers;

namespace Donakunn.MessagingOverQueue.Persistence.Repositories;

/// <summary>
/// Repository interface for outbox operations.
/// Provides provider-agnostic access to outbox message storage.
/// </summary>
public interface IOutboxRepository
{
    /// <summary>
    /// Adds a message to the outbox.
    /// </summary>
    /// <param name="entry">The outbox entry to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task AddAsync(MessageStoreEntry entry, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds a message to the outbox within an existing transaction context.
    /// Use this when you need transactional consistency with other database operations.
    /// </summary>
    /// <param name="entry">The outbox entry to add.</param>
    /// <param name="transactionContext">The transaction context to use.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task AddAsync(MessageStoreEntry entry, ITransactionContext transactionContext, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acquires a lock on messages for processing.
    /// Returns messages that are pending or have expired locks.
    /// </summary>
    /// <param name="batchSize">Maximum number of messages to acquire.</param>
    /// <param name="lockDuration">How long to hold the lock.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<MessageStoreEntry>> AcquireLockAsync(int batchSize, TimeSpan lockDuration, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acquires a lock on messages for processing, filtered by assigned partitions.
    /// Messages are partitioned by QueueName to maintain ordering within a queue.
    /// </summary>
    /// <param name="batchSize">Maximum number of messages to acquire.</param>
    /// <param name="lockDuration">How long to hold the lock.</param>
    /// <param name="assignedPartitions">The partition numbers this worker is responsible for.</param>
    /// <param name="partitionCount">Total number of partitions.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<MessageStoreEntry>> AcquireLockAsync(
        int batchSize,
        TimeSpan lockDuration,
        int[]? assignedPartitions,
        int partitionCount,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks a message as published.
    /// </summary>
    /// <param name="messageId">The message ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MarkAsPublishedAsync(Guid messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks multiple messages as published in a single batch operation.
    /// </summary>
    /// <param name="messageIds">The message IDs to mark as published.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MarkAsPublishedBatchAsync(IEnumerable<Guid> messageIds, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks a message as failed.
    /// </summary>
    /// <param name="messageId">The message ID.</param>
    /// <param name="error">The error message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MarkAsFailedAsync(Guid messageId, string error, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks multiple messages as failed in a single batch operation.
    /// </summary>
    /// <param name="failures">Collection of message IDs and their error messages.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MarkAsFailedBatchAsync(IEnumerable<(Guid Id, string Error)> failures, CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases the lock on a message, returning it to pending status.
    /// </summary>
    /// <param name="messageId">The message ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ReleaseLockAsync(Guid messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Cleans up old processed messages.
    /// </summary>
    /// <param name="retentionPeriod">How long to retain processed messages.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CleanupAsync(TimeSpan retentionPeriod, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a new transaction context for batch operations.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ITransactionContext> BeginTransactionAsync(CancellationToken cancellationToken = default);
}

