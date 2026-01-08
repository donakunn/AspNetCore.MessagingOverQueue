namespace Donakunn.MessagingOverQueue.Topology.Attributes;

/// <summary>
/// Specifies the consumer queue configuration for a message handler.
/// When applied to a handler class, this attribute allows customizing the queue
/// that the handler consumes from, separate from the message's default topology.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class ConsumerQueueAttribute : Attribute
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
    /// Message TTL in milliseconds. If -1, no TTL is set.
    /// </summary>
    public int MessageTtlMs { get; init; } = -1;

    /// <summary>
    /// Maximum number of messages in the queue. If -1, no limit.
    /// </summary>
    public int MaxLength { get; init; } = -1;

    /// <summary>
    /// Maximum queue size in bytes. If -1, no limit.
    /// </summary>
    public long MaxLengthBytes { get; init; } = -1;

    /// <summary>
    /// The queue type (classic, quorum, stream).
    /// </summary>
    public QueueType QueueType { get; init; } = QueueType.Classic;

    /// <summary>
    /// The prefetch count for this consumer. If -1, uses default.
    /// </summary>
    public ushort PrefetchCount { get; init; } = 10;

    /// <summary>
    /// The maximum concurrency for this consumer. If -1, uses default.
    /// </summary>
    public int MaxConcurrency { get; init; } = 1;

    /// <summary>
    /// Creates a new consumer queue attribute.
    /// </summary>
    public ConsumerQueueAttribute() { }

    /// <summary>
    /// Creates a new consumer queue attribute with the specified name.
    /// </summary>
    /// <param name="name">The queue name.</param>
    public ConsumerQueueAttribute(string name)
    {
        Name = name;
    }
}
