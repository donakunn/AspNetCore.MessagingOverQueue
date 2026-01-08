using Donakunn.MessagingOverQueue.Abstractions.Consuming;
using Donakunn.MessagingOverQueue.Abstractions.Messages;
using Donakunn.MessagingOverQueue.Topology.Abstractions;
using Donakunn.MessagingOverQueue.Topology.Attributes;
using System.Reflection;

namespace Donakunn.MessagingOverQueue.Topology;

/// <summary>
/// Scans assemblies for message types and handlers to auto-discover topology.
/// </summary>
public sealed class TopologyScanner : ITopologyScanner
{
    private static readonly Type MessageInterface = typeof(IMessage);
    private static readonly Type CommandInterface = typeof(ICommand);
    private static readonly Type EventInterface = typeof(IEvent);
    private static readonly Type QueryInterface = typeof(IQuery<>);
    private static readonly Type HandlerOpenGenericType = typeof(IMessageHandler<>);

    /// <inheritdoc />
    public IReadOnlyCollection<MessageTypeInfo> ScanForMessageTypes(params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);

        var messageTypes = new List<MessageTypeInfo>();

        foreach (var assembly in assemblies)
        {
            ScanAssemblyForMessageTypes(assembly, messageTypes);
        }

        return messageTypes.AsReadOnly();
    }

    /// <inheritdoc />
    public IReadOnlyCollection<HandlerTypeInfo> ScanForHandlers(params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);

        var handlers = new List<HandlerTypeInfo>();

        foreach (var assembly in assemblies)
        {
            ScanAssemblyForHandlers(assembly, handlers);
        }

        return handlers.AsReadOnly();
    }

    /// <inheritdoc />
    public IReadOnlyCollection<HandlerTopologyInfo> ScanForHandlerTopology(params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);

        var handlerTopologies = new List<HandlerTopologyInfo>();

        foreach (var assembly in assemblies)
        {
            ScanAssemblyForHandlerTopology(assembly, handlerTopologies);
        }

        return handlerTopologies.AsReadOnly();
    }

    private static void ScanAssemblyForMessageTypes(Assembly assembly, List<MessageTypeInfo> messageTypes)
    {
        try
        {
            var types = assembly.GetTypes().Where(IsValidMessageType);

            foreach (var type in types)
            {
                var messageAttribute = type.GetCustomAttribute<MessageAttribute>();
                if (messageAttribute?.AutoDiscover == false)
                    continue;

                var attributes = type.GetCustomAttributes().ToList();

                messageTypes.Add(new MessageTypeInfo
                {
                    MessageType = type,
                    IsCommand = CommandInterface.IsAssignableFrom(type),
                    IsEvent = EventInterface.IsAssignableFrom(type),
                    IsQuery = IsQueryType(type),
                    Attributes = attributes.AsReadOnly()
                });
            }
        }
        catch (ReflectionTypeLoadException ex)
        {
            ProcessLoadedTypes(ex, messageTypes);
        }
    }

    private static void ScanAssemblyForHandlers(Assembly assembly, List<HandlerTypeInfo> handlers)
    {
        try
        {
            var types = assembly.GetTypes().Where(IsValidHandlerType);

            foreach (var type in types)
            {
                var handlerInfo = ExtractHandlerInfo(type);
                if (handlerInfo != null)
                {
                    handlers.Add(handlerInfo);
                }
            }
        }
        catch (ReflectionTypeLoadException ex)
        {
            var loadedTypes = ex.Types.Where(t => t != null && IsValidHandlerType(t!));
            foreach (var type in loadedTypes)
            {
                var handlerInfo = ExtractHandlerInfo(type!);
                if (handlerInfo != null)
                {
                    handlers.Add(handlerInfo);
                }
            }
        }
    }

    private static void ScanAssemblyForHandlerTopology(Assembly assembly, List<HandlerTopologyInfo> handlerTopologies)
    {
        try
        {
            var types = assembly.GetTypes().Where(IsValidHandlerType);

            foreach (var type in types)
            {
                var topologyInfo = ExtractHandlerTopologyInfo(type);
                if (topologyInfo != null)
                {
                    handlerTopologies.Add(topologyInfo);
                }
            }
        }
        catch (ReflectionTypeLoadException ex)
        {
            var loadedTypes = ex.Types.Where(t => t != null && IsValidHandlerType(t!));
            foreach (var type in loadedTypes)
            {
                var topologyInfo = ExtractHandlerTopologyInfo(type!);
                if (topologyInfo != null)
                {
                    handlerTopologies.Add(topologyInfo);
                }
            }
        }
    }

    private static HandlerTypeInfo? ExtractHandlerInfo(Type type)
    {
        var handlerInterface = type.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType &&
                                i.GetGenericTypeDefinition() == HandlerOpenGenericType);

        if (handlerInterface == null)
            return null;

        var messageType = handlerInterface.GetGenericArguments()[0];
        var attributes = type.GetCustomAttributes().ToList();

        return new HandlerTypeInfo
        {
            HandlerType = type,
            MessageType = messageType,
            Attributes = attributes.AsReadOnly()
        };
    }

    private static HandlerTopologyInfo? ExtractHandlerTopologyInfo(Type type)
    {
        var handlerInterface = type.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType &&
                                i.GetGenericTypeDefinition() == HandlerOpenGenericType);

        if (handlerInterface == null)
            return null;

        var messageType = handlerInterface.GetGenericArguments()[0];
        var handlerAttributes = type.GetCustomAttributes().ToList();
        var messageAttributes = messageType.GetCustomAttributes().ToList();

        // Check for MessageAttribute.AutoDiscover on the message type
        var messageAttribute = messageType.GetCustomAttribute<MessageAttribute>();
        if (messageAttribute?.AutoDiscover == false)
            return null;

        // Extract consumer queue configuration from handler
        var consumerQueueAttr = type.GetCustomAttribute<ConsumerQueueAttribute>();
        var consumerQueueConfig = ExtractConsumerQueueConfig(consumerQueueAttr);

        return new HandlerTopologyInfo
        {
            HandlerType = type,
            MessageType = messageType,
            HandlerAttributes = handlerAttributes.AsReadOnly(),
            MessageAttributes = messageAttributes.AsReadOnly(),
            ConsumerQueueConfig = consumerQueueConfig,
            IsCommand = CommandInterface.IsAssignableFrom(messageType),
            IsEvent = EventInterface.IsAssignableFrom(messageType),
            IsQuery = IsQueryType(messageType)
        };
    }

    private static ConsumerQueueInfo? ExtractConsumerQueueConfig(ConsumerQueueAttribute? attr)
    {
        if (attr == null)
            return null;

        return new ConsumerQueueInfo
        {
            QueueName = attr.Name,
            Durable = attr.Durable,
            Exclusive = attr.Exclusive,
            AutoDelete = attr.AutoDelete,
            MessageTtlMs = attr.MessageTtlMs > 0 ? attr.MessageTtlMs : null,
            MaxLength = attr.MaxLength > 0 ? attr.MaxLength : null,
            MaxLengthBytes = attr.MaxLengthBytes > 0 ? attr.MaxLengthBytes : null,
            QueueType = ConvertQueueType(attr.QueueType),
            PrefetchCount = attr.PrefetchCount,
            MaxConcurrency = attr.MaxConcurrency
        };
    }

    private static string? ConvertQueueType(QueueType queueType)
    {
        return queueType switch
        {
            QueueType.Quorum => "quorum",
            QueueType.Stream => "stream",
            QueueType.Lazy => "lazy",
            QueueType.Classic => null,
            _ => null
        };
    }

    private static void ProcessLoadedTypes(ReflectionTypeLoadException ex, List<MessageTypeInfo> messageTypes)
    {
        var loadedTypes = ex.Types.Where(t => t != null && IsValidMessageType(t!));
        foreach (var type in loadedTypes)
        {
            if (type is null) continue;

            var attributes = type.GetCustomAttributes().ToList();

            messageTypes.Add(new MessageTypeInfo
            {
                MessageType = type,
                IsCommand = CommandInterface.IsAssignableFrom(type),
                IsEvent = EventInterface.IsAssignableFrom(type),
                IsQuery = IsQueryType(type),
                Attributes = attributes.AsReadOnly()
            });
        }
    }

    private static bool IsValidMessageType(Type type)
    {
        return type is { IsClass: true, IsAbstract: false } &&
               MessageInterface.IsAssignableFrom(type);
    }

    private static bool IsValidHandlerType(Type type)
    {
        if (type is not { IsClass: true, IsAbstract: false })
            return false;

        return type.GetInterfaces()
            .Any(i => i.IsGenericType &&
                     i.GetGenericTypeDefinition() == HandlerOpenGenericType);
    }

    private static bool IsQueryType(Type type)
    {
        return type.GetInterfaces()
            .Any(i => i.IsGenericType &&
                     i.GetGenericTypeDefinition() == QueryInterface);
    }
}
