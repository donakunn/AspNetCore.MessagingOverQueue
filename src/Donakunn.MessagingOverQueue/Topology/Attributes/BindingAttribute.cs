namespace MessagingOverQueue.src.Topology.Attributes;

/// <summary>
/// Specifies a binding configuration between exchange and queue.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = true)]
public sealed class BindingAttribute : Attribute
{
    /// <summary>
    /// The routing key pattern for the binding.
    /// </summary>
    public string RoutingKey { get; }

    /// <summary>
    /// Optional exchange name to bind from. If null, uses the message's exchange.
    /// </summary>
    public string? FromExchange { get; init; }

    /// <summary>
    /// Optional queue name to bind to. If null, uses the message's queue.
    /// </summary>
    public string? ToQueue { get; init; }

    /// <summary>
    /// Creates a new binding attribute with the specified routing key.
    /// </summary>
    /// <param name="routingKey">The routing key pattern.</param>
    public BindingAttribute(string routingKey)
    {
        RoutingKey = routingKey;
    }
}
