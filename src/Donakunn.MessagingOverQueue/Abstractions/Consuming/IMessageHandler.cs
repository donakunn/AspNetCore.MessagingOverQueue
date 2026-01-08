using Donakunn.MessagingOverQueue.Abstractions.Messages;

namespace Donakunn.MessagingOverQueue.Abstractions.Consuming;

/// <summary>
/// Interface for handling messages of a specific type.
/// </summary>
/// <typeparam name="TMessage">The type of message to handle.</typeparam>
public interface IMessageHandler<in TMessage> where TMessage : IMessage
{
    /// <summary>
    /// Handles the message.
    /// </summary>
    /// <param name="message">The message to handle.</param>
    /// <param name="context">The message context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task HandleAsync(TMessage message, IMessageContext context, CancellationToken cancellationToken = default);
}

/// <summary>
/// Provides context information about the message being processed.
/// </summary>
public interface IMessageContext
{
    /// <summary>
    /// The message ID.
    /// </summary>
    Guid MessageId { get; }

    /// <summary>
    /// The correlation ID.
    /// </summary>
    string? CorrelationId { get; }

    /// <summary>
    /// The causation ID.
    /// </summary>
    string? CausationId { get; }

    /// <summary>
    /// The queue the message was received from.
    /// </summary>
    string QueueName { get; }

    /// <summary>
    /// The exchange the message was published to.
    /// </summary>
    string? ExchangeName { get; }

    /// <summary>
    /// The routing key used for the message.
    /// </summary>
    string? RoutingKey { get; }

    /// <summary>
    /// Headers associated with the message.
    /// </summary>
    IReadOnlyDictionary<string, object> Headers { get; }

    /// <summary>
    /// The number of times this message has been delivered.
    /// </summary>
    int DeliveryCount { get; }

    /// <summary>
    /// The timestamp when the message was received.
    /// </summary>
    DateTime ReceivedAt { get; }

    /// <summary>
    /// Stores custom data in the context.
    /// </summary>
    void SetData<T>(string key, T value);

    /// <summary>
    /// Retrieves custom data from the context.
    /// </summary>
    T? GetData<T>(string key);
}

