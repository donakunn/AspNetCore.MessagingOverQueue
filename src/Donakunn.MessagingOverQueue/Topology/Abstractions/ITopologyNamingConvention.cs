namespace Donakunn.MessagingOverQueue.Topology.Abstractions;

/// <summary>
/// Defines naming conventions for RabbitMQ topology.
/// </summary>
public interface ITopologyNamingConvention
{
    /// <summary>
    /// Gets the exchange name for a message type.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    /// <returns>The exchange name.</returns>
    string GetExchangeName(Type messageType);

    /// <summary>
    /// Gets the queue name for a message type.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    /// <returns>The queue name.</returns>
    string GetQueueName(Type messageType);

    /// <summary>
    /// Gets the routing key for a message type.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    /// <returns>The routing key.</returns>
    string GetRoutingKey(Type messageType);

    /// <summary>
    /// Gets the dead letter exchange name.
    /// </summary>
    /// <param name="sourceQueueName">The source queue name.</param>
    /// <returns>The dead letter exchange name.</returns>
    string GetDeadLetterExchangeName(string sourceQueueName);

    /// <summary>
    /// Gets the dead letter queue name.
    /// </summary>
    /// <param name="sourceQueueName">The source queue name.</param>
    /// <returns>The dead letter queue name.</returns>
    string GetDeadLetterQueueName(string sourceQueueName);

    /// <summary>
    /// Gets the consumer queue name for a handler type.
    /// Uses handler-specific naming when service name is provided.
    /// </summary>
    /// <param name="handlerType">The handler type.</param>
    /// <param name="messageType">The message type being handled.</param>
    /// <returns>The consumer queue name.</returns>
    string GetConsumerQueueName(Type handlerType, Type messageType) => GetQueueName(messageType);
}
