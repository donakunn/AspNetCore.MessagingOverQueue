using System.Reflection;

namespace MessagingOverQueue.Topology.Abstractions;

/// <summary>
/// Scans assemblies for message types and handlers to auto-discover topology.
/// </summary>
public interface ITopologyScanner
{
    /// <summary>
    /// Scans the specified assemblies for message types.
    /// </summary>
    /// <param name="assemblies">The assemblies to scan.</param>
    /// <returns>Collection of discovered message types.</returns>
    IReadOnlyCollection<MessageTypeInfo> ScanForMessageTypes(params Assembly[] assemblies);
    
    /// <summary>
    /// Scans the specified assemblies for message handlers.
    /// </summary>
    /// <param name="assemblies">The assemblies to scan.</param>
    /// <returns>Collection of discovered handler types.</returns>
    IReadOnlyCollection<HandlerTypeInfo> ScanForHandlers(params Assembly[] assemblies);
}

/// <summary>
/// Information about a discovered message type.
/// </summary>
public sealed class MessageTypeInfo
{
    /// <summary>
    /// The message type.
    /// </summary>
    public Type MessageType { get; init; } = null!;
    
    /// <summary>
    /// Whether this is a command type.
    /// </summary>
    public bool IsCommand { get; init; }
    
    /// <summary>
    /// Whether this is an event type.
    /// </summary>
    public bool IsEvent { get; init; }
    
    /// <summary>
    /// Whether this is a query type.
    /// </summary>
    public bool IsQuery { get; init; }
    
    /// <summary>
    /// Custom attributes applied to the message type.
    /// </summary>
    public IReadOnlyCollection<Attribute> Attributes { get; init; } = Array.Empty<Attribute>();
}

/// <summary>
/// Information about a discovered handler type.
/// </summary>
public sealed class HandlerTypeInfo
{
    /// <summary>
    /// The handler type.
    /// </summary>
    public Type HandlerType { get; init; } = null!;
    
    /// <summary>
    /// The message type this handler handles.
    /// </summary>
    public Type MessageType { get; init; } = null!;
    
    /// <summary>
    /// Custom attributes applied to the handler type.
    /// </summary>
    public IReadOnlyCollection<Attribute> Attributes { get; init; } = Array.Empty<Attribute>();
}
