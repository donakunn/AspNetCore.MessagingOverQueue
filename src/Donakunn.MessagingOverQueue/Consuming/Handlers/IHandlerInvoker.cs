using Donakunn.MessagingOverQueue.Abstractions.Consuming;
using Donakunn.MessagingOverQueue.Abstractions.Messages;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.Persistence.Repositories;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Donakunn.MessagingOverQueue.Consuming.Handlers;

/// <summary>
/// Abstraction for invoking message handlers without reflection at runtime.
/// </summary>
public interface IHandlerInvoker
{
    /// <summary>
    /// The message type this invoker handles.
    /// </summary>
    Type MessageType { get; }

    /// <summary>
    /// Invokes all registered handlers for the message.
    /// </summary>
    /// <param name="serviceProvider">The scoped service provider.</param>
    /// <param name="message">The message to handle.</param>
    /// <param name="context">The message context.</param>
    /// <param name="contextData">Additional context data from the consume pipeline.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task InvokeAsync(
        IServiceProvider serviceProvider,
        IMessage message,
        IMessageContext context,
        IReadOnlyDictionary<string, object>? contextData,
        CancellationToken cancellationToken);
}

/// <summary>
/// Generic handler invoker that provides strongly-typed handler invocation with per-handler idempotency support.
/// </summary>
/// <typeparam name="TMessage">The message type.</typeparam>
internal sealed class HandlerInvoker<TMessage> : IHandlerInvoker
    where TMessage : IMessage
{
    public Type MessageType => typeof(TMessage);

    public async Task InvokeAsync(
        IServiceProvider serviceProvider,
        IMessage message,
        IMessageContext context,
        IReadOnlyDictionary<string, object>? contextData,
        CancellationToken cancellationToken)
    {
        var handlers = serviceProvider.GetServices<IMessageHandler<TMessage>>();
        var idempotencyContext = IdempotencyContext.TryCreate(contextData);

        foreach (var handler in handlers)
        {
            var handlerType = handler.GetType().FullName ?? handler.GetType().Name;

            if (idempotencyContext != null)
            {
                // Atomically try to mark as processed before invoking the handler.
                // This prevents race conditions where multiple instances of the same message
                // could be processed simultaneously.
                var wasMarked = await idempotencyContext.InboxRepository.TryMarkAsProcessedAsync(
                    message,
                    handlerType,
                    cancellationToken);

                if (!wasMarked)
                {
                    idempotencyContext.Logger.LogDebug(
                        "Message {MessageId} already processed by {HandlerType}, skipping",
                        message.Id,
                        handlerType);
                    continue;
                }
            }

            // Invoke the handler
            await handler.HandleAsync((TMessage)message, context, cancellationToken);

            if (idempotencyContext != null)
            {
                idempotencyContext.Logger.LogDebug(
                    "Message {MessageId} processed by {HandlerType}",
                    message.Id,
                    handlerType);
            }
        }
    }
}

/// <summary>
/// Encapsulates idempotency context data extracted from the consume pipeline.
/// </summary>
internal sealed class IdempotencyContext
{
    public required IInboxRepository InboxRepository { get; init; }
    public required ILogger Logger { get; init; }

    /// <summary>
    /// Tries to create an idempotency context from the pipeline context data.
    /// </summary>
    /// <param name="contextData">The context data dictionary.</param>
    /// <returns>An <see cref="IdempotencyContext"/> if the required data is present; otherwise, null.</returns>
    public static IdempotencyContext? TryCreate(IReadOnlyDictionary<string, object>? contextData)
    {
        if (contextData == null)
            return null;

        if (!contextData.TryGetValue(IdempotencyMiddleware.InboxRepositoryKey, out var repoObj) ||
            repoObj is not IInboxRepository inboxRepository)
            return null;

        if (!contextData.TryGetValue(IdempotencyMiddleware.LoggerKey, out var loggerObj) ||
            loggerObj is not ILogger logger)
            return null;

        return new IdempotencyContext
        {
            InboxRepository = inboxRepository,
            Logger = logger
        };
    }
}
