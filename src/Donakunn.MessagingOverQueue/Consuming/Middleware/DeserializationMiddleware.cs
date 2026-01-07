using MessagingOverQueue.src.Abstractions.Messages;
using MessagingOverQueue.src.Abstractions.Serialization;
using Microsoft.Extensions.Logging;

namespace MessagingOverQueue.src.Consuming.Middleware;

/// <summary>
/// Middleware that deserializes incoming messages.
/// </summary>
public class DeserializationMiddleware : IConsumeMiddleware
{
    private readonly IMessageSerializer _serializer;
    private readonly IMessageTypeResolver _typeResolver;
    private readonly ILogger<DeserializationMiddleware> _logger;

    public DeserializationMiddleware(
        IMessageSerializer serializer,
        IMessageTypeResolver typeResolver,
        ILogger<DeserializationMiddleware> logger)
    {
        _serializer = serializer;
        _typeResolver = typeResolver;
        _logger = logger;
    }

    public async Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
    {
        try
        {
            var messageTypeName = context.Headers.TryGetValue("message-type", out var typeHeader)
                ? typeHeader?.ToString()
                : null;

            if (string.IsNullOrEmpty(messageTypeName))
            {
                _logger.LogWarning("Message does not contain message-type header, delivery tag: {DeliveryTag}",
                    context.DeliveryTag);
                context.ShouldReject = true;
                context.RequeueOnReject = false;
                return;
            }

            var messageType = _typeResolver.ResolveType(messageTypeName);
            if (messageType == null)
            {
                _logger.LogWarning("Could not resolve message type: {MessageType}", messageTypeName);
                context.ShouldReject = true;
                context.RequeueOnReject = false;
                return;
            }

            context.MessageType = messageType;
            context.Message = (IMessage?)_serializer.Deserialize(context.Body, messageType);

            if (context.Message == null)
            {
                _logger.LogWarning("Failed to deserialize message of type {MessageType}", messageTypeName);
                context.ShouldReject = true;
                context.RequeueOnReject = false;
                return;
            }

            _logger.LogDebug("Deserialized message {MessageId} of type {MessageType}",
                context.Message.Id, messageType.Name);

            await next(context, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deserializing message, delivery tag: {DeliveryTag}", context.DeliveryTag);
            context.Exception = ex;
            context.ShouldReject = true;
            context.RequeueOnReject = false;
        }
    }
}