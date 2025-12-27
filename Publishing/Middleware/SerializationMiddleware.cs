using MessagingOverQueue.Abstractions.Serialization;
using Microsoft.Extensions.Logging;

namespace MessagingOverQueue.Publishing.Middleware;

/// <summary>
/// Middleware that serializes messages before publishing.
/// </summary>
public class SerializationMiddleware : IPublishMiddleware
{
    private readonly IMessageSerializer _serializer;
    private readonly ILogger<SerializationMiddleware> _logger;

    public SerializationMiddleware(IMessageSerializer serializer, ILogger<SerializationMiddleware> logger)
    {
        _serializer = serializer;
        _logger = logger;
    }

    public async Task InvokeAsync(PublishContext context, Func<PublishContext, CancellationToken, Task> next, CancellationToken cancellationToken)
    {
        _logger.LogDebug("Serializing message {MessageId} of type {MessageType}", 
            context.Message.Id, context.MessageType.Name);

        context.Body = _serializer.Serialize(context.Message, context.MessageType);
        context.ContentType = _serializer.ContentType;
        
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

