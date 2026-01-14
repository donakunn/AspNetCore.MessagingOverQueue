using Donakunn.MessagingOverQueue.Abstractions.Messages;

namespace Donakunn.MessagingOverQueue.Persistence.Repositories;

/// <summary>
/// Repository interface for inbox operations (idempotency tracking).
/// Provides provider-agnostic access to track processed messages.
/// </summary>
public interface IInboxRepository
{
    /// <summary>
    /// Checks if a message has already been processed by a specific handler.
    /// </summary>
    /// <param name="messageId">The message ID.</param>
    /// <param name="handlerType">The handler type name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> HasBeenProcessedAsync(Guid messageId, string handlerType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks a message as processed by a specific handler.
    /// </summary>
    /// <param name="message">The message that was processed.</param>
    /// <param name="handlerType">The handler type name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MarkAsProcessedAsync(IMessage message, string handlerType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to mark a message as processed by a specific handler atomically.
    /// Uses database constraints to prevent race conditions.
    /// </summary>
    /// <param name="message">The message to mark as processed.</param>
    /// <param name="handlerType">The handler type name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the message was successfully marked as processed; false if it was already processed.</returns>
    Task<bool> TryMarkAsProcessedAsync(IMessage message, string handlerType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Cleans up old inbox records.
    /// </summary>
    /// <param name="retentionPeriod">How long to retain processed message records.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CleanupAsync(TimeSpan retentionPeriod, CancellationToken cancellationToken = default);
}

