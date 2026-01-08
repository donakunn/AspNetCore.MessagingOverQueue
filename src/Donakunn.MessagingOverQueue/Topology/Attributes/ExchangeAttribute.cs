namespace Donakunn.MessagingOverQueue.Topology.Attributes;

/// <summary>
/// Specifies the exchange configuration for a message type.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class ExchangeAttribute : Attribute
{
    /// <summary>
    /// The exchange name. If null, convention-based naming will be used.
    /// </summary>
    public string? Name { get; init; }

    /// <summary>
    /// The exchange type. Defaults to "topic".
    /// </summary>
    public ExchangeType Type { get; init; } = ExchangeType.Topic;

    /// <summary>
    /// Whether the exchange is durable. Defaults to true.
    /// </summary>
    public bool Durable { get; init; } = true;

    /// <summary>
    /// Whether to auto-delete the exchange. Defaults to false.
    /// </summary>
    public bool AutoDelete { get; init; }

    /// <summary>
    /// Creates a new exchange attribute.
    /// </summary>
    public ExchangeAttribute() { }

    /// <summary>
    /// Creates a new exchange attribute with the specified name.
    /// </summary>
    /// <param name="name">The exchange name.</param>
    public ExchangeAttribute(string name)
    {
        Name = name;
    }
}

/// <summary>
/// RabbitMQ exchange types.
/// </summary>
public enum ExchangeType
{
    /// <summary>
    /// Direct exchange - routes to queues with exact matching routing key.
    /// </summary>
    Direct,

    /// <summary>
    /// Topic exchange - routes to queues with pattern matching routing key.
    /// </summary>
    Topic,

    /// <summary>
    /// Fanout exchange - routes to all bound queues.
    /// </summary>
    Fanout,

    /// <summary>
    /// Headers exchange - routes based on message headers.
    /// </summary>
    Headers
}
