using System.Collections.Concurrent;
using Donakunn.MessagingOverQueue.Abstractions.Messages;
using Donakunn.MessagingOverQueue.Abstractions.Serialization;
using Microsoft.Extensions.Logging;

namespace Donakunn.MessagingOverQueue.Consuming.Middleware;

/// <summary>
/// Middleware that deserializes incoming messages.
/// </summary>
public class DeserializationMiddleware : IOrderedConsumeMiddleware
{
    private readonly IMessageSerializer _serializer;
    private readonly IMessageTypeResolver _typeResolver;
    private readonly ILogger<DeserializationMiddleware> _logger;
    private readonly ConcurrentDictionary<string, Type?> _resolvedTypeCache = new();

    public DeserializationMiddleware(
        IMessageSerializer serializer,
        IMessageTypeResolver typeResolver,
        ILogger<DeserializationMiddleware> logger)
    {
        _serializer = serializer;
        _typeResolver = typeResolver;
        _logger = logger;
    }

    /// <inheritdoc />
    public int Order => MiddlewareOrder.Deserialization;

    public async Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
    {
        // Deserialize the message first - wrap only deserialization in try-catch
        try
        {
            // Try to get message type from headers first, then fall back to context.Data
            // (Redis Streams stores it in Data, RabbitMQ uses Headers)
            var messageTypeName = context.Headers.TryGetValue("message-type", out var typeHeader)
                ? typeHeader?.ToString()
                : null;

            if (string.IsNullOrEmpty(messageTypeName) && context.Data.TryGetValue("message-type", out var dataTypeValue))
            {
                messageTypeName = dataTypeValue?.ToString();
            }

            if (string.IsNullOrEmpty(messageTypeName))
            {
                _logger.LogWarning("Message does not contain message-type header, delivery tag: {DeliveryTag}",
                    context.DeliveryTag);
                context.ShouldReject = true;
                context.RequeueOnReject = false;
                return;
            }

            var messageType = _resolvedTypeCache.GetOrAdd(
                messageTypeName,
                typeName => _typeResolver.ResolveType(typeName));

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
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deserializing message, delivery tag: {DeliveryTag}", context.DeliveryTag);
            context.Exception = ex;
            context.ShouldReject = true;
            context.RequeueOnReject = false;
            return;
        }

        // Call the next middleware/handler - let exceptions propagate
        // so that failed messages stay in pending for reclaiming
        await next(context, cancellationToken);
    }
}