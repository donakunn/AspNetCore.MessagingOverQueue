namespace Donakunn.MessagingOverQueue.Topology.Attributes;

/// <summary>
/// Specifies a custom consumer group name for Redis Streams.
/// When not specified, the default queue name from topology conventions is used.
/// </summary>
/// <remarks>
/// This attribute is only used by the Redis Streams provider.
/// RabbitMQ provider ignores this attribute.
/// </remarks>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class RedisConsumerGroupAttribute : Attribute
{
    /// <summary>
    /// Gets the custom consumer group name.
    /// </summary>
    public string GroupName { get; }

    /// <summary>
    /// Creates a new instance specifying a custom consumer group name.
    /// </summary>
    /// <param name="groupName">The consumer group name to use for Redis Streams.</param>
    public RedisConsumerGroupAttribute(string groupName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupName);
        GroupName = groupName;
    }
}
