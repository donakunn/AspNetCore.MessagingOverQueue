using MessagingOverQueue.src.Abstractions.Serialization;
using Microsoft.Extensions.Logging;

namespace MessagingOverQueue.src.Publishing.Middleware;

/// <summary>
/// Middleware that serializes messages before publishing.
/// </summary>
public class SerializationMiddleware(IMessageSerializer serializer, ILogger<SerializationMiddleware> logger) : IPublishMiddleware
{
    public async Task InvokeAsync(PublishContext context, Func<PublishContext, CancellationToken, Task> next, CancellationToken cancellationToken)
    {
        logger.LogDebug("Serializing message {MessageId} of type {MessageType}",
            context.Message.Id, context.MessageType.Name);

        context.Body = serializer.Serialize(context.Message, context.MessageType);
        context.ContentType = serializer.ContentType;

        context.Headers["message-type"] = context.Message.MessageType;
        context.Headers["message-id"] = context.Message.Id.ToString();
        context.Headers["timestamp"] = context.Message.Timestamp.ToString("O");

        if (context.Message.CorrelationId != null)
            context.Headers["correlation-id"] = context.Message.CorrelationId;

        if (context.Message.CausationId != null)
            context.Headers["causation-id"] = context.Message.CausationId;

        await next(context, cancellationToken);
    }
}

