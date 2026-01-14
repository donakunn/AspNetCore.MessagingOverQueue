using Donakunn.MessagingOverQueue.Persistence.Entities;
using Donakunn.MessagingOverQueue.Persistence.Providers;

namespace Donakunn.MessagingOverQueue.Persistence.Repositories;

/// <summary>
/// Provider-based implementation of the outbox repository.
/// Delegates all operations to the configured <see cref="IMessageStoreProvider"/>.
/// </summary>
public sealed class OutboxRepository : IOutboxRepository
{
    private readonly IMessageStoreProvider _provider;

    public OutboxRepository(IMessageStoreProvider provider)
    {
        _provider = provider;
    }

    public Task AddAsync(MessageStoreEntry entry, CancellationToken cancellationToken = default)
    {
        ValidateOutboxEntry(entry);
        return _provider.AddAsync(entry, cancellationToken);
    }

    public Task AddAsync(MessageStoreEntry entry, ITransactionContext transactionContext, CancellationToken cancellationToken = default)
    {
        ValidateOutboxEntry(entry);
        return _provider.AddAsync(entry, transactionContext, cancellationToken);
    }

    public Task<IReadOnlyList<MessageStoreEntry>> AcquireLockAsync(int batchSize, TimeSpan lockDuration, CancellationToken cancellationToken = default)
    {
        return _provider.AcquireOutboxLockAsync(batchSize, lockDuration, cancellationToken);
    }

    public Task MarkAsPublishedAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        return _provider.MarkAsPublishedAsync(messageId, cancellationToken);
    }

    public Task MarkAsFailedAsync(Guid messageId, string error, CancellationToken cancellationToken = default)
    {
        return _provider.MarkAsFailedAsync(messageId, error, cancellationToken);
    }

    public Task ReleaseLockAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        return _provider.ReleaseLockAsync(messageId, cancellationToken);
    }

    public Task CleanupAsync(TimeSpan retentionPeriod, CancellationToken cancellationToken = default)
    {
        return _provider.CleanupAsync(MessageDirection.Outbox, retentionPeriod, cancellationToken);
    }

    public Task<ITransactionContext> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        return _provider.BeginTransactionAsync(cancellationToken);
    }

    private static void ValidateOutboxEntry(MessageStoreEntry entry)
    {
        if (entry.Direction != MessageDirection.Outbox)
        {
            throw new ArgumentException("Entry must be an outbox entry.", nameof(entry));
        }
    }
}

