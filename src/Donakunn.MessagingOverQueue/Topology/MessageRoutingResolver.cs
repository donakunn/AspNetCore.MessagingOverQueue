using MessagingOverQueue.src.Abstractions.Messages;
using MessagingOverQueue.src.Topology.Abstractions;

namespace MessagingOverQueue.src.Topology;

/// <summary>
/// Resolves routing information for messages using the topology provider.
/// </summary>
public interface IMessageRoutingResolver
{
    /// <summary>
    /// Gets the exchange name for a message type.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <returns>The exchange name.</returns>
    string GetExchangeName<TMessage>() where TMessage : IMessage;

    /// <summary>
    /// Gets the exchange name for a message type.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    /// <returns>The exchange name.</returns>
    string GetExchangeName(Type messageType);

    /// <summary>
    /// Gets the queue name for a message type.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <returns>The queue name.</returns>
    string GetQueueName<TMessage>() where TMessage : IMessage;

    /// <summary>
    /// Gets the queue name for a message type.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    /// <returns>The queue name.</returns>
    string GetQueueName(Type messageType);

    /// <summary>
    /// Gets the routing key for a message type.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <returns>The routing key.</returns>
    string GetRoutingKey<TMessage>() where TMessage : IMessage;

    /// <summary>
    /// Gets the routing key for a message type.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    /// <returns>The routing key.</returns>
    string GetRoutingKey(Type messageType);

    /// <summary>
    /// Gets the full topology for a message type.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <returns>The topology definition.</returns>
    TopologyDefinition GetTopology<TMessage>() where TMessage : IMessage;

    /// <summary>
    /// Gets the full topology for a message type.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    /// <returns>The topology definition.</returns>
    TopologyDefinition GetTopology(Type messageType);
}

/// <summary>
/// Default implementation of message routing resolver.
/// </summary>
/// <remarks>
/// Creates a new instance with the specified topology provider.
/// </remarks>
public sealed class MessageRoutingResolver(ITopologyProvider topologyProvider) : IMessageRoutingResolver
{
    private readonly ITopologyProvider _topologyProvider = topologyProvider ?? throw new ArgumentNullException(nameof(topologyProvider));

    /// <inheritdoc />
    public string GetExchangeName<TMessage>() where TMessage : IMessage
    {
        return GetExchangeName(typeof(TMessage));
    }

    /// <inheritdoc />
    public string GetExchangeName(Type messageType)
    {
        var topology = _topologyProvider.GetTopology(messageType);
        return topology.Exchange.Name;
    }

    /// <inheritdoc />
    public string GetQueueName<TMessage>() where TMessage : IMessage
    {
        return GetQueueName(typeof(TMessage));
    }

    /// <inheritdoc />
    public string GetQueueName(Type messageType)
    {
        var topology = _topologyProvider.GetTopology(messageType);
        return topology.Queue.Name;
    }

    /// <inheritdoc />
    public string GetRoutingKey<TMessage>() where TMessage : IMessage
    {
        return GetRoutingKey(typeof(TMessage));
    }

    /// <inheritdoc />
    public string GetRoutingKey(Type messageType)
    {
        var topology = _topologyProvider.GetTopology(messageType);
        return topology.RoutingKey;
    }

    /// <inheritdoc />
    public TopologyDefinition GetTopology<TMessage>() where TMessage : IMessage
    {
        return GetTopology(typeof(TMessage));
    }

    /// <inheritdoc />
    public TopologyDefinition GetTopology(Type messageType)
    {
        return _topologyProvider.GetTopology(messageType);
    }
}
