using MessagingOverQueue.Abstractions.Messages;
using MessagingOverQueue.Persistence.Entities;
using Microsoft.EntityFrameworkCore;

namespace MessagingOverQueue.Persistence.Repositories;

/// <summary>
/// EF Core implementation of the inbox repository.
/// </summary>
public class InboxRepository<TContext> : IInboxRepository where TContext : DbContext, IOutboxDbContext
{
    private readonly TContext _context;

    public InboxRepository(TContext context)
    {
        _context = context;
    }

    public async Task<bool> HasBeenProcessedAsync(Guid messageId, string handlerType, CancellationToken cancellationToken = default)
    {
        return await _context.InboxMessages
            .AnyAsync(m => m.Id == messageId && m.HandlerType == handlerType, cancellationToken);
    }

    public async Task MarkAsProcessedAsync(IMessage message, string handlerType, CancellationToken cancellationToken = default)
    {
        var inboxMessage = new InboxMessage
        {
            Id = message.Id,
            MessageType = message.MessageType,
            HandlerType = handlerType,
            ProcessedAt = DateTime.UtcNow,
            CorrelationId = message.CorrelationId
        };

        await _context.InboxMessages.AddAsync(inboxMessage, cancellationToken);
        await _context.SaveChangesAsync(cancellationToken);
    }

    public async Task CleanupAsync(TimeSpan retentionPeriod, CancellationToken cancellationToken = default)
    {
        var cutoffDate = DateTime.UtcNow.Subtract(retentionPeriod);
        
        await _context.InboxMessages
            .Where(m => m.ProcessedAt < cutoffDate)
            .ExecuteDeleteAsync(cancellationToken);
    }
}

