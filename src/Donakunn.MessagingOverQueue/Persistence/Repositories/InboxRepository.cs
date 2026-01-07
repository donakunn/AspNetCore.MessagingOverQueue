using MessagingOverQueue.src.Abstractions.Messages;
using MessagingOverQueue.src.Persistence.Entities;
using Microsoft.EntityFrameworkCore;

namespace MessagingOverQueue.src.Persistence.Repositories;

/// <summary>
/// EF Core implementation of the inbox repository.
/// </summary>
public class InboxRepository<TContext>(TContext context) : IInboxRepository where TContext : DbContext, IOutboxDbContext
{
    public async Task<bool> HasBeenProcessedAsync(Guid messageId, string handlerType, CancellationToken cancellationToken = default)
    {
        return await context.InboxMessages
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

        await context.InboxMessages.AddAsync(inboxMessage, cancellationToken);
        await context.SaveChangesAsync(cancellationToken);
    }

    public async Task CleanupAsync(TimeSpan retentionPeriod, CancellationToken cancellationToken = default)
    {
        var cutoffDate = DateTime.UtcNow.Subtract(retentionPeriod);

        await context.InboxMessages
            .Where(m => m.ProcessedAt < cutoffDate)
            .ExecuteDeleteAsync(cancellationToken);
    }
}

