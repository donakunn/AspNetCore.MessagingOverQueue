using Donakunn.MessagingOverQueue.Abstractions.Messages;
using Donakunn.MessagingOverQueue.Persistence.Entities;
using Donakunn.MessagingOverQueue.Persistence.Providers;

namespace Donakunn.MessagingOverQueue.Persistence.Repositories;

/// <summary>
/// Provider-based implementation of the inbox repository.
/// Delegates all operations to the configured <see cref="IMessageStoreProvider"/>.
/// </summary>
public sealed class InboxRepository : IInboxRepository
{
    private readonly IMessageStoreProvider _provider;

    public InboxRepository(IMessageStoreProvider provider)
    {
        _provider = provider;
    }

    public Task<bool> HasBeenProcessedAsync(Guid messageId, string handlerType, CancellationToken cancellationToken = default)
    {
        return _provider.ExistsInboxEntryAsync(messageId, handlerType, cancellationToken);
    }

    public async Task MarkAsProcessedAsync(IMessage message, string handlerType, CancellationToken cancellationToken = default)
    {
        var entry = MessageStoreEntry.CreateInboxEntry(
            message.Id,
            message.MessageType,
            handlerType,
            message.CorrelationId);

        await _provider.AddAsync(entry, cancellationToken);
    }

    public async Task<bool> TryMarkAsProcessedAsync(IMessage message, string handlerType, CancellationToken cancellationToken = default)
    {
        var entry = MessageStoreEntry.CreateInboxEntry(
            message.Id,
            message.MessageType,
            handlerType,
            message.CorrelationId);

        return await _provider.TryAddAsync(entry, cancellationToken);
    }

    public Task CleanupAsync(TimeSpan retentionPeriod, CancellationToken cancellationToken = default)
    {
        return _provider.CleanupAsync(MessageDirection.Inbox, retentionPeriod, cancellationToken);
    }
}

