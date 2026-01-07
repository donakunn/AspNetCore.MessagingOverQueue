namespace MessagingOverQueue.src.Topology.Abstractions;

/// <summary>
/// Provides topology configuration for a specific message type.
/// </summary>
public interface ITopologyProvider
{
    /// <summary>
    /// Gets the topology configuration for a message type.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    /// <returns>The topology configuration.</returns>
    TopologyDefinition GetTopology(Type messageType);

    /// <summary>
    /// Gets all topology definitions from registered message types.
    /// </summary>
    /// <returns>Collection of topology definitions.</returns>
    IReadOnlyCollection<TopologyDefinition> GetAllTopologies();
}

/// <summary>
/// Represents a complete topology definition including exchanges, queues, and bindings.
/// </summary>
public sealed class TopologyDefinition
{
    /// <summary>
    /// The message type this topology is for.
    /// </summary>
    public Type MessageType { get; init; } = null!;

    /// <summary>
    /// The exchange definition.
    /// </summary>
    public ExchangeDefinition Exchange { get; init; } = null!;

    /// <summary>
    /// The queue definition.
    /// </summary>
    public QueueDefinition Queue { get; init; } = null!;

    /// <summary>
    /// The binding definition.
    /// </summary>
    public BindingDefinition Binding { get; init; } = null!;

    /// <summary>
    /// Optional dead letter configuration.
    /// </summary>
    public DeadLetterDefinition? DeadLetter { get; init; }

    /// <summary>
    /// The routing key used for publishing.
    /// </summary>
    public string RoutingKey { get; init; } = string.Empty;
}

/// <summary>
/// Exchange definition.
/// </summary>
public sealed class ExchangeDefinition
{
    public string Name { get; init; } = string.Empty;
    public string Type { get; init; } = "topic";
    public bool Durable { get; init; } = true;
    public bool AutoDelete { get; init; }
    public IDictionary<string, object?>? Arguments { get; init; }
}

/// <summary>
/// Queue definition.
/// </summary>
public sealed class QueueDefinition
{
    public string Name { get; init; } = string.Empty;
    public bool Durable { get; init; } = true;
    public bool Exclusive { get; init; }
    public bool AutoDelete { get; init; }
    public int? MessageTtl { get; init; }
    public int? MaxLength { get; init; }
    public long? MaxLengthBytes { get; init; }
    public string? OverflowBehavior { get; init; }
    public string? QueueType { get; init; }
    public IDictionary<string, object>? Arguments { get; init; }
}

/// <summary>
/// Binding definition.
/// </summary>
public sealed class BindingDefinition
{
    public string ExchangeName { get; init; } = string.Empty;
    public string QueueName { get; init; } = string.Empty;
    public string RoutingKey { get; init; } = string.Empty;
    public IDictionary<string, object?>? Arguments { get; init; }
}

/// <summary>
/// Dead letter configuration.
/// </summary>
public sealed class DeadLetterDefinition
{
    public string ExchangeName { get; init; } = string.Empty;
    public string QueueName { get; init; } = string.Empty;
    public string? RoutingKey { get; init; }
}
