using System.Reflection;

namespace MessagingOverQueue.src.Topology.Abstractions;

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

    /// <summary>
    /// Scans the specified assemblies for message handlers with full topology information.
    /// This is the primary method for handler-based topology discovery.
    /// </summary>
    /// <param name="assemblies">The assemblies to scan.</param>
    /// <returns>Collection of discovered handler topology information.</returns>
    IReadOnlyCollection<HandlerTopologyInfo> ScanForHandlerTopology(params Assembly[] assemblies);
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

/// <summary>
/// Complete topology information discovered from a handler.
/// </summary>
public sealed class HandlerTopologyInfo
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
    public IReadOnlyCollection<Attribute> HandlerAttributes { get; init; } = Array.Empty<Attribute>();

    /// <summary>
    /// Custom attributes applied to the message type.
    /// </summary>
    public IReadOnlyCollection<Attribute> MessageAttributes { get; init; } = Array.Empty<Attribute>();

    /// <summary>
    /// Consumer queue configuration extracted from ConsumerQueueAttribute.
    /// </summary>
    public ConsumerQueueInfo? ConsumerQueueConfig { get; init; }

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
}

/// <summary>
/// Consumer queue configuration extracted from ConsumerQueueAttribute.
/// </summary>
public sealed class ConsumerQueueInfo
{
    /// <summary>
    /// The name of the queue.
    /// </summary>
    public string? QueueName { get; init; }

    /// <summary>
    /// Whether the queue is durable.
    /// </summary>
    public bool Durable { get; init; } = true;

    /// <summary>
    /// Whether the queue is exclusive.
    /// </summary>
    public bool Exclusive { get; init; }

    /// <summary>
    /// Whether the queue should automatically delete itself when no longer in use.
    /// </summary>
    public bool AutoDelete { get; init; }

    /// <summary>
    /// The time-to-live for messages in the queue, in milliseconds.
    /// </summary>
    public int? MessageTtlMs { get; init; }

    /// <summary>
    /// The maximum number of messages that can be stored in the queue.
    /// </summary>
    public int? MaxLength { get; init; }

    /// <summary>
    /// The maximum number of bytes that can be stored in the queue.
    /// </summary>
    public long? MaxLengthBytes { get; init; }

    /// <summary>
    /// The type of the queue.
    /// </summary>
    public string? QueueType { get; init; }

    /// <summary>
    /// The number of messages to prefetch at a time.
    /// </summary>
    public ushort PrefetchCount { get; init; } = 10;

    /// <summary>
    /// The maximum concurrency level for processing messages from the queue.
    /// </summary>
    public int MaxConcurrency { get; init; } = 1;
}
