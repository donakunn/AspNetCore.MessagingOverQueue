using Donakunn.MessagingOverQueue.Persistence.Repositories;
using Microsoft.Extensions.Logging;

namespace Donakunn.MessagingOverQueue.Consuming.Middleware;

/// <summary>
/// Middleware that ensures idempotent message processing.
/// Works in conjunction with <see cref="IdempotentHandlerInvoker{TMessage}"/> to provide per-handler idempotency.
/// </summary>
public class IdempotencyMiddleware(
    IInboxRepository inboxRepository,
    ILogger<IdempotencyMiddleware> logger) : IOrderedConsumeMiddleware
{
    /// <inheritdoc />
    public int Order => MiddlewareOrder.Idempotency;
    /// <summary>
    /// Key used to store the <see cref="IInboxRepository"/> in the context data.
    /// </summary>
    internal const string InboxRepositoryKey = "IdempotencyMiddleware.InboxRepository";

    /// <summary>
    /// Key used to store the logger in the context data.
    /// </summary>
    internal const string LoggerKey = "IdempotencyMiddleware.Logger";

    public async Task InvokeAsync(
        ConsumeContext context,
        Func<ConsumeContext, CancellationToken, Task> next,
        CancellationToken cancellationToken)
    {
        // Store the inbox repository and logger in context for use by HandlerInvoker.
        // This must happen BEFORE the null check because IdempotencyMiddleware runs before
        // DeserializationMiddleware, so context.Message is always null at this point.
        // The actual idempotency check happens in HandlerInvoker after deserialization.
        context.Data[InboxRepositoryKey] = inboxRepository;
        context.Data[LoggerKey] = logger;

        await next(context, cancellationToken);
    }
}

