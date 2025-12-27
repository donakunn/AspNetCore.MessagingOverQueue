using AsyncronousComunication.Abstractions.Messages;
using AsyncronousComunication.Abstractions.Serialization;
using Microsoft.Extensions.Logging;

namespace AsyncronousComunication.Consuming.Middleware;

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

///// <summary>
///// Interface for resolving message types from type names.
///// </summary>
//public interface IMessageTypeResolver
//{
//    /// <summary>
//    /// Resolves a type from its name.
//    /// </summary>
//    Type? ResolveType(string typeName);
    
//    /// <summary>
//    /// Registers a message type.
//    /// </summary>
//    void RegisterType<T>() where T : IMessage;
    
//    /// <summary>
//    /// Registers a message type.
//    /// </summary>
//    void RegisterType(Type type);
//}

///// <summary>
///// Default implementation of message type resolver.
///// </summary>
//public class MessageTypeResolver : IMessageTypeResolver
//{
//    private readonly Dictionary<string, Type> _typeMap = new();
//    private readonly object _lock = new();

//    public Type? ResolveType(string typeName)
//    {
//        lock (_lock)
//        {
//            if (_typeMap.TryGetValue(typeName, out var type))
//                return type;
//        }

//        // Try to resolve from assembly qualified name
//        var resolvedType = Type.GetType(typeName, throwOnError: false);
//        if (resolvedType != null)
//        {
//            RegisterType(resolvedType);
//            return resolvedType;
//        }

//        // Try to find in loaded assemblies
//        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
//        {
//            resolvedType = assembly.GetType(typeName.Split(',')[0]);
//            if (resolvedType != null)
//            {
//                RegisterType(resolvedType);
//                return resolvedType;
//            }
//        }

//        return null;
//    }

//    public void RegisterType<T>() where T : IMessage
//    {
//        RegisterType(typeof(T));
//    }

//    public void RegisterType(Type type)
//    {
//        lock (_lock)
//        {
//            var key = type.AssemblyQualifiedName ?? type.FullName ?? type.Name;
//            _typeMap[key] = type;
            
//            // Also register by full name for easier resolution
//            if (type.FullName != null)
//                _typeMap[type.FullName] = type;
//        }
//    }
//}

