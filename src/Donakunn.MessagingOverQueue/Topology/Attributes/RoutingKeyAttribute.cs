namespace MessagingOverQueue.src.Topology.Attributes;

/// <summary>
/// Specifies the routing key for a message type.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class RoutingKeyAttribute : Attribute
{
    /// <summary>
    /// The routing key pattern.
    /// </summary>
    public string Pattern { get; }

    /// <summary>
    /// Creates a new routing key attribute.
    /// </summary>
    /// <param name="pattern">The routing key pattern.</param>
    public RoutingKeyAttribute(string pattern)
    {
        Pattern = pattern;
    }
}
