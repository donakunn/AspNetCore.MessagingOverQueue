using Donakunn.MessagingOverQueue.Persistence.Entities;

namespace Donakunn.MessagingOverQueue.Persistence.Providers;

/// <summary>
/// Abstraction for message store persistence operations.
/// Implementations provide database-specific functionality (SQL Server, PostgreSQL, etc.).
/// </summary>
public interface IMessageStoreProvider
{
    /// <summary>
    /// Adds a message entry to the store.
    /// </summary>
    /// <param name="entry">The message entry to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task AddAsync(MessageStoreEntry entry, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds a message entry within an existing transaction/connection context.
    /// </summary>
    /// <param name="entry">The message entry to add.</param>
    /// <param name="transactionContext">The transaction context to use.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task AddAsync(MessageStoreEntry entry, ITransactionContext transactionContext, CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to add a message entry to the store atomically.
    /// Returns false if the entry already exists (based on primary key).
    /// </summary>
    /// <param name="entry">The message entry to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the entry was added; false if it already exists.</returns>
    Task<bool> TryAddAsync(MessageStoreEntry entry, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a message entry by ID and direction.
    /// </summary>
    /// <param name="id">The message ID.</param>
    /// <param name="direction">The message direction.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<MessageStoreEntry?> GetByIdAsync(Guid id, MessageDirection direction, CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks if an inbox entry exists for a message and handler combination.
    /// </summary>
    /// <param name="messageId">The message ID.</param>
    /// <param name="handlerType">The handler type name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> ExistsInboxEntryAsync(Guid messageId, string handlerType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acquires a lock on pending outbox messages for processing.
    /// Returns messages that are either pending or have expired locks.
    /// </summary>
    /// <param name="batchSize">Maximum number of messages to acquire.</param>
    /// <param name="lockDuration">How long to hold the lock.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<MessageStoreEntry>> AcquireOutboxLockAsync(
        int batchSize,
        TimeSpan lockDuration,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates an outbox message status to published.
    /// </summary>
    /// <param name="messageId">The message ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MarkAsPublishedAsync(Guid messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates an outbox message status to failed.
    /// </summary>
    /// <param name="messageId">The message ID.</param>
    /// <param name="error">The error message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MarkAsFailedAsync(Guid messageId, string error, CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases a lock on an outbox message, returning it to pending status.
    /// </summary>
    /// <param name="messageId">The message ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ReleaseLockAsync(Guid messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Cleans up old processed messages.
    /// </summary>
    /// <param name="direction">The direction to cleanup (Outbox or Inbox).</param>
    /// <param name="retentionPeriod">How long to retain processed messages.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CleanupAsync(MessageDirection direction, TimeSpan retentionPeriod, CancellationToken cancellationToken = default);

    /// <summary>
    /// Ensures the message store schema exists (creates tables if needed).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task EnsureSchemaAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a new transaction context for batch operations.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ITransactionContext> BeginTransactionAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Represents a transaction context for batch operations.
/// </summary>
public interface ITransactionContext : IAsyncDisposable
{
    /// <summary>
    /// Commits the transaction.
    /// </summary>
    Task CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Rolls back the transaction.
    /// </summary>
    Task RollbackAsync(CancellationToken cancellationToken = default);
}
