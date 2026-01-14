using Donakunn.MessagingOverQueue.Persistence.Entities;
using Donakunn.MessagingOverQueue.Persistence.Providers;
using System.Collections.Concurrent;

namespace MessagingOverQueue.Test.Integration.Infrastructure;

/// <summary>
/// In-memory implementation of <see cref="IMessageStoreProvider"/> for testing.
/// Thread-safe and supports all provider operations.
/// </summary>
public sealed class InMemoryMessageStoreProvider : IMessageStoreProvider
{
    private readonly ConcurrentDictionary<(Guid Id, MessageDirection Direction, string? HandlerType), MessageStoreEntry> _entries = new();
    private readonly object _lockObject = new();

    public Task AddAsync(MessageStoreEntry entry, CancellationToken cancellationToken = default)
    {
        var key = GetKey(entry);
        if (!_entries.TryAdd(key, CloneEntry(entry)))
        {
            throw new InvalidOperationException($"Entry with Id={entry.Id}, Direction={entry.Direction}, HandlerType={entry.HandlerType} already exists.");
        }
        return Task.CompletedTask;
    }

    public Task AddAsync(MessageStoreEntry entry, ITransactionContext transactionContext, CancellationToken cancellationToken = default)
    {
        // In-memory doesn't support real transactions, just add directly
        return AddAsync(entry, cancellationToken);
    }

    public Task<MessageStoreEntry?> GetByIdAsync(Guid id, MessageDirection direction, CancellationToken cancellationToken = default)
    {
        var entry = _entries.Values.FirstOrDefault(e => e.Id == id && e.Direction == direction);
        return Task.FromResult(entry != null ? CloneEntry(entry) : null);
    }

    public Task<bool> ExistsInboxEntryAsync(Guid messageId, string handlerType, CancellationToken cancellationToken = default)
    {
        var key = (messageId, MessageDirection.Inbox, handlerType);
        return Task.FromResult(_entries.ContainsKey(key));
    }

    public Task<IReadOnlyList<MessageStoreEntry>> AcquireOutboxLockAsync(
        int batchSize,
        TimeSpan lockDuration,
        CancellationToken cancellationToken = default)
    {
        var lockToken = Guid.NewGuid().ToString("N");
        var now = DateTime.UtcNow;
        var lockExpiresAt = now.Add(lockDuration);
        var result = new List<MessageStoreEntry>();

        lock (_lockObject)
        {
            var candidates = _entries.Values
                .Where(e => e.Direction == MessageDirection.Outbox &&
                           (e.Status == MessageStatus.Pending ||
                            (e.Status == MessageStatus.Processing && e.LockExpiresAt < now)))
                .OrderBy(e => e.CreatedAt)
                .Take(batchSize)
                .ToList();

            foreach (var entry in candidates)
            {
                entry.Status = MessageStatus.Processing;
                entry.LockToken = lockToken;
                entry.LockExpiresAt = lockExpiresAt;
                result.Add(CloneEntry(entry));
            }
        }

        return Task.FromResult<IReadOnlyList<MessageStoreEntry>>(result);
    }

    public Task MarkAsPublishedAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        var entry = _entries.Values.FirstOrDefault(e => e.Id == messageId && e.Direction == MessageDirection.Outbox);
        if (entry != null)
        {
            entry.Status = MessageStatus.Published;
            entry.ProcessedAt = DateTime.UtcNow;
            entry.LockToken = null;
            entry.LockExpiresAt = null;
        }
        return Task.CompletedTask;
    }

    public Task MarkAsFailedAsync(Guid messageId, string error, CancellationToken cancellationToken = default)
    {
        var entry = _entries.Values.FirstOrDefault(e => e.Id == messageId && e.Direction == MessageDirection.Outbox);
        if (entry != null)
        {
            entry.Status = MessageStatus.Failed;
            entry.RetryCount++;
            entry.LastError = error.Length > 4000 ? error[..4000] : error;
            entry.LockToken = null;
            entry.LockExpiresAt = null;
        }
        return Task.CompletedTask;
    }

    public Task ReleaseLockAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        var entry = _entries.Values.FirstOrDefault(e => e.Id == messageId && e.Direction == MessageDirection.Outbox);
        if (entry != null)
        {
            entry.Status = MessageStatus.Pending;
            entry.LockToken = null;
            entry.LockExpiresAt = null;
        }
        return Task.CompletedTask;
    }

    public Task CleanupAsync(MessageDirection direction, TimeSpan retentionPeriod, CancellationToken cancellationToken = default)
    {
        var cutoffDate = DateTime.UtcNow.Subtract(retentionPeriod);

        var toRemove = _entries.Values
            .Where(e => e.Direction == direction &&
                       e.ProcessedAt.HasValue &&
                       e.ProcessedAt.Value < cutoffDate &&
                       (direction == MessageDirection.Inbox || e.Status == MessageStatus.Published))
            .Select(e => GetKey(e))
            .ToList();

        foreach (var key in toRemove)
        {
            _entries.TryRemove(key, out _);
        }

        return Task.CompletedTask;
    }

    public Task EnsureSchemaAsync(CancellationToken cancellationToken = default)
    {
        // No schema to create for in-memory
        return Task.CompletedTask;
    }

    public Task<ITransactionContext> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult<ITransactionContext>(new InMemoryTransactionContext());
    }

    /// <summary>
    /// Gets all entries in the store. For testing purposes.
    /// </summary>
    public IReadOnlyCollection<MessageStoreEntry> GetAllEntries() => _entries.Values.ToList();

    /// <summary>
    /// Clears all entries. For testing purposes.
    /// </summary>
    public void Clear() => _entries.Clear();

    /// <summary>
    /// Gets outbox entries by status. For testing purposes.
    /// </summary>
    public IReadOnlyList<MessageStoreEntry> GetOutboxEntriesByStatus(MessageStatus status)
    {
        return _entries.Values
            .Where(e => e.Direction == MessageDirection.Outbox && e.Status == status)
            .ToList();
    }

    private static (Guid Id, MessageDirection Direction, string? HandlerType) GetKey(MessageStoreEntry entry)
    {
        return (entry.Id, entry.Direction, entry.HandlerType);
    }

    private static MessageStoreEntry CloneEntry(MessageStoreEntry entry)
    {
        return new MessageStoreEntry
        {
            Id = entry.Id,
            Direction = entry.Direction,
            MessageType = entry.MessageType,
            Payload = entry.Payload,
            ExchangeName = entry.ExchangeName,
            RoutingKey = entry.RoutingKey,
            Headers = entry.Headers,
            HandlerType = entry.HandlerType,
            CreatedAt = entry.CreatedAt,
            ProcessedAt = entry.ProcessedAt,
            Status = entry.Status,
            RetryCount = entry.RetryCount,
            LastError = entry.LastError,
            LockToken = entry.LockToken,
            LockExpiresAt = entry.LockExpiresAt,
            CorrelationId = entry.CorrelationId
        };
    }

    public Task<bool> TryAddAsync(MessageStoreEntry entry, CancellationToken cancellationToken = default)
    {
        //TODO: implement when needed.
        throw new NotImplementedException();
    }
}

/// <summary>
/// In-memory transaction context that does nothing (no-op).
/// </summary>
internal sealed class InMemoryTransactionContext : ITransactionContext
{
    public Task CommitAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    public Task RollbackAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
