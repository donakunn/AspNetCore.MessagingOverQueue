namespace Donakunn.MessagingOverQueue.Topology.Attributes;

/// <summary>
/// Configures dead letter handling for a message type.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class DeadLetterAttribute : Attribute
{
    /// <summary>
    /// Whether to enable dead letter handling. Defaults to true.
    /// </summary>
    public bool Enabled { get; init; } = true;

    /// <summary>
    /// The dead letter exchange name. If null, convention-based naming will be used.
    /// </summary>
    public string? ExchangeName { get; init; }

    /// <summary>
    /// The dead letter queue name. If null, convention-based naming will be used.
    /// </summary>
    public string? QueueName { get; init; }

    /// <summary>
    /// The dead letter routing key. If null, the original routing key is preserved.
    /// </summary>
    public string? RoutingKey { get; init; }

    /// <summary>
    /// Creates a new dead letter attribute with dead lettering enabled.
    /// </summary>
    public DeadLetterAttribute() { }

    /// <summary>
    /// Creates a new dead letter attribute with the specified exchange name.
    /// </summary>
    /// <param name="exchangeName">The dead letter exchange name.</param>
    public DeadLetterAttribute(string exchangeName)
    {
        ExchangeName = exchangeName;
    }
}
