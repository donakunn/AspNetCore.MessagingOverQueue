using MessagingOverQueue.src.Persistence.Repositories;
using Microsoft.Extensions.Logging;

namespace MessagingOverQueue.src.Consuming.Middleware;

/// <summary>
/// Middleware that ensures idempotent message processing.
/// </summary>
public class IdempotencyMiddleware : IConsumeMiddleware
{
    private readonly IInboxRepository _inboxRepository;
    private readonly ILogger<IdempotencyMiddleware> _logger;

    public IdempotencyMiddleware(
        IInboxRepository inboxRepository,
        ILogger<IdempotencyMiddleware> logger)
    {
        _inboxRepository = inboxRepository;
        _logger = logger;
    }

    public async Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
    {
        if (context.Message == null)
        {
            await next(context, cancellationToken);
            return;
        }

        var handlerType = context.Data.TryGetValue("HandlerType", out var ht)
            ? ht?.ToString() ?? "Unknown"
            : "Unknown";

        // Check if message was already processed
        var alreadyProcessed = await _inboxRepository.HasBeenProcessedAsync(
            context.Message.Id,
            handlerType,
            cancellationToken);

        if (alreadyProcessed)
        {
            _logger.LogDebug("Message {MessageId} already processed by {HandlerType}, skipping",
                context.Message.Id, handlerType);
            return;
        }

        await next(context, cancellationToken);

        // Mark as processed after successful handling
        if (!context.ShouldReject && context.Exception == null)
        {
            await _inboxRepository.MarkAsProcessedAsync(context.Message, handlerType, cancellationToken);
        }
    }
}

