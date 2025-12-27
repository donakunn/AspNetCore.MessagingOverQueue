using System.Reflection;
using AsyncronousComunication.Abstractions.Consuming;
using AsyncronousComunication.Abstractions.Messages;
using AsyncronousComunication.Topology.Abstractions;
using AsyncronousComunication.Topology.Attributes;

namespace AsyncronousComunication.Topology;

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
            try
            {
                var types = assembly.GetTypes()
                    .Where(IsValidMessageType);

                foreach (var type in types)
                {
                    var messageAttribute = type.GetCustomAttribute<MessageAttribute>();
                    
                    // Skip if explicitly marked as not auto-discoverable
                    if (messageAttribute?.AutoDiscover == false)
                        continue;

                    var attributes = type.GetCustomAttributes().ToList();
                    
                    messageTypes.Add(new MessageTypeInfo
                    {
                        MessageType = type,
                        IsCommand = CommandInterface.IsAssignableFrom(type),
                        IsEvent = EventInterface.IsAssignableFrom(type),
                        IsQuery = QueryInterface.IsAssignableFrom(type),
                        Attributes = attributes.AsReadOnly()
                    });
                }
            }
            catch (ReflectionTypeLoadException ex)
            {
                // Handle assemblies that can't be fully loaded
                var loadedTypes = ex.Types.Where(t => t != null && IsValidMessageType(t!));
                foreach (var type in loadedTypes)
                {
                    var attributes = type!.GetCustomAttributes().ToList();
                    
                    messageTypes.Add(new MessageTypeInfo
                    {
                        MessageType = type,
                        IsCommand = CommandInterface.IsAssignableFrom(type),
                        IsEvent = EventInterface.IsAssignableFrom(type),
                        IsQuery = QueryInterface.IsAssignableFrom(type),
                        Attributes = attributes.AsReadOnly()
                    });
                }
            }
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
            try
            {
                var types = assembly.GetTypes()
                    .Where(IsValidHandlerType);

                foreach (var type in types)
                {
                    var handlerInterface = type.GetInterfaces()
                        .FirstOrDefault(i => i.IsGenericType && 
                                            i.GetGenericTypeDefinition() == HandlerOpenGenericType);

                    if (handlerInterface == null)
                        continue;

                    var messageType = handlerInterface.GetGenericArguments()[0];
                    var attributes = type.GetCustomAttributes().ToList();

                    handlers.Add(new HandlerTypeInfo
                    {
                        HandlerType = type,
                        MessageType = messageType,
                        Attributes = attributes.AsReadOnly()
                    });
                }
            }
            catch (ReflectionTypeLoadException ex)
            {
                // Handle assemblies that can't be fully loaded
                var loadedTypes = ex.Types.Where(t => t != null && IsValidHandlerType(t!));
                foreach (var type in loadedTypes)
                {
                    var handlerInterface = type!.GetInterfaces()
                        .FirstOrDefault(i => i.IsGenericType && 
                                            i.GetGenericTypeDefinition() == HandlerOpenGenericType);

                    if (handlerInterface == null)
                        continue;

                    var messageType = handlerInterface.GetGenericArguments()[0];
                    var attributes = type.GetCustomAttributes().ToList();

                    handlers.Add(new HandlerTypeInfo
                    {
                        HandlerType = type,
                        MessageType = messageType,
                        Attributes = attributes.AsReadOnly()
                    });
                }
            }
        }

        return handlers.AsReadOnly();
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
}
