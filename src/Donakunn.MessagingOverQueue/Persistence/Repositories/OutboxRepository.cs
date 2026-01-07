using MessagingOverQueue.src.Persistence.Entities;
using Microsoft.EntityFrameworkCore;

namespace MessagingOverQueue.src.Persistence.Repositories;

/// <summary>
/// EF Core implementation of the outbox repository.
/// </summary>
public class OutboxRepository<TContext> : IOutboxRepository where TContext : DbContext, IOutboxDbContext
{
    private readonly TContext _context;

    public OutboxRepository(TContext context)
    {
        _context = context;
    }

    public async Task AddAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        await _context.OutboxMessages.AddAsync(message, cancellationToken);
    }

    public async Task<IReadOnlyList<OutboxMessage>> GetPendingMessagesAsync(int batchSize, CancellationToken cancellationToken = default)
    {
        return await _context.OutboxMessages
            .Where(m => m.Status == OutboxMessageStatus.Pending)
            .OrderBy(m => m.CreatedAt)
            .Take(batchSize)
            .ToListAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<OutboxMessage>> AcquireLockAsync(int batchSize, TimeSpan lockDuration, CancellationToken cancellationToken = default)
    {
        var lockToken = Guid.NewGuid().ToString("N");
        var now = DateTime.UtcNow;
        var lockExpiresAt = now.Add(lockDuration);

        // Get messages that are pending or have expired locks
        var messages = await _context.OutboxMessages
            .Where(m =>
                (m.Status == OutboxMessageStatus.Pending) ||
                (m.Status == OutboxMessageStatus.Processing && m.LockExpiresAt < now))
            .OrderBy(m => m.CreatedAt)
            .Take(batchSize)
            .ToListAsync(cancellationToken);

        foreach (var message in messages)
        {
            message.Status = OutboxMessageStatus.Processing;
            message.LockToken = lockToken;
            message.LockExpiresAt = lockExpiresAt;
        }

        await _context.SaveChangesAsync(cancellationToken);

        return messages;
    }

    public async Task MarkAsPublishedAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        var message = await _context.OutboxMessages.FindAsync(new object[] { messageId }, cancellationToken);
        if (message != null)
        {
            message.Status = OutboxMessageStatus.Published;
            message.ProcessedAt = DateTime.UtcNow;
            message.LockToken = null;
            message.LockExpiresAt = null;
        }
    }

    public async Task MarkAsFailedAsync(Guid messageId, string error, CancellationToken cancellationToken = default)
    {
        var message = await _context.OutboxMessages.FindAsync(new object[] { messageId }, cancellationToken);
        if (message != null)
        {
            message.RetryCount++;
            message.LastError = error.Length > 4000 ? error[..4000] : error;
            message.Status = OutboxMessageStatus.Failed;
            message.LockToken = null;
            message.LockExpiresAt = null;
        }
    }

    public async Task ReleaseLockAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        var message = await _context.OutboxMessages.FindAsync(new object[] { messageId }, cancellationToken);
        if (message != null)
        {
            message.Status = OutboxMessageStatus.Pending;
            message.LockToken = null;
            message.LockExpiresAt = null;
        }
    }

    public async Task CleanupAsync(TimeSpan retentionPeriod, CancellationToken cancellationToken = default)
    {
        var cutoffDate = DateTime.UtcNow.Subtract(retentionPeriod);

        await _context.OutboxMessages
            .Where(m => m.Status == OutboxMessageStatus.Published && m.ProcessedAt < cutoffDate)
            .ExecuteDeleteAsync(cancellationToken);
    }

    public async Task SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        await _context.SaveChangesAsync(cancellationToken);
    }
}

