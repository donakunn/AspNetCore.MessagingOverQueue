namespace Donakunn.MessagingOverQueue.Topology.Attributes;

/// <summary>
/// Specifies the queue configuration for a message type.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class QueueAttribute : Attribute
{
    /// <summary>
    /// The queue name. If null, convention-based naming will be used.
    /// </summary>
    public string? Name { get; init; }

    /// <summary>
    /// Whether the queue is durable. Defaults to true.
    /// </summary>
    public bool Durable { get; init; } = true;

    /// <summary>
    /// Whether the queue is exclusive. Defaults to false.
    /// </summary>
    public bool Exclusive { get; init; }

    /// <summary>
    /// Whether to auto-delete the queue. Defaults to false.
    /// </summary>
    public bool AutoDelete { get; init; }

    /// <summary>
    /// Message TTL in milliseconds. If null, no TTL is set.
    /// </summary>
    public int MessageTtlMs { get; init; } = -1;

    /// <summary>
    /// Maximum number of messages in the queue. If null, no limit.
    /// </summary>
    public int MaxLength { get; init; } = -1;

    /// <summary>
    /// Maximum queue size in bytes. If null, no limit.
    /// </summary>
    public long MaxLengthBytes { get; init; } = -1;

    /// <summary>
    /// The queue type (classic, quorum, stream).
    /// </summary>
    public QueueType QueueType { get; init; } = QueueType.Classic;

    /// <summary>
    /// Creates a new queue attribute.
    /// </summary>
    public QueueAttribute() { }

    /// <summary>
    /// Creates a new queue attribute with the specified name.
    /// </summary>
    /// <param name="name">The queue name.</param>
    public QueueAttribute(string name)
    {
        Name = name;
    }
}

/// <summary>
/// RabbitMQ queue types.
/// </summary>
public enum QueueType
{
    /// <summary>
    /// Classic queue - standard RabbitMQ queue.
    /// </summary>
    Classic,

    /// <summary>
    /// Quorum queue - highly available, replicated queue.
    /// </summary>
    Quorum,

    /// <summary>
    /// Stream queue - high-throughput, append-only log.
    /// </summary>
    Stream,

    /// <summary>
    /// Lazy queue - stores messages to disk as soon as possible.
    /// </summary>
    Lazy
}
