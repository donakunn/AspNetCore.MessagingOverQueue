using Donakunn.MessagingOverQueue.Abstractions.Messages;

namespace Donakunn.MessagingOverQueue.Abstractions.Consuming;

/// <summary>
/// Handles messages of type <typeparamref name="TMessage"/>.
/// </summary>
/// <typeparam name="TMessage">The message type to handle.</typeparam>
/// <remarks>
/// <para><b>Idempotency Requirement:</b> Handlers MUST be idempotent. The library provides
/// at-least-once delivery semantics, meaning the same message may be delivered multiple times
/// in edge cases (network failures, process restarts, consumer crashes during processing).</para>
///
/// <para><b>Scoped Lifetime:</b> Handlers are resolved from a scoped DI container per message.
/// Each message gets a fresh handler instance with isolated dependencies (e.g., DbContext).
/// This ensures no shared state between concurrent message processing.</para>
///
/// <para><b>Multiple Handlers:</b> Multiple handlers can be registered for the same message type.
/// When a message arrives, all registered handlers for that type execute sequentially.
/// Each handler has independent idempotency tracking via the inbox pattern when enabled.</para>
///
/// <para><b>Error Handling:</b> If a handler throws an exception, the message will be requeued
/// (based on configuration) or moved to a dead letter queue after max retry attempts.</para>
/// </remarks>
/// <example>
/// <code>
/// public class OrderCreatedHandler : IMessageHandler&lt;OrderCreatedEvent&gt;
/// {
///     private readonly AppDbContext _context;
///
///     public OrderCreatedHandler(AppDbContext context)
///     {
///         _context = context;
///     }
///
///     public async Task HandleAsync(
///         OrderCreatedEvent message,
///         IMessageContext context,
///         CancellationToken cancellationToken)
///     {
///         // Handler receives fresh DbContext per message
///         await _context.Orders.AddAsync(new Order { Id = message.OrderId }, cancellationToken);
///         await _context.SaveChangesAsync(cancellationToken);
///     }
/// }
/// </code>
/// </example>
public interface IMessageHandler<in TMessage> where TMessage : IMessage
{
    /// <summary>
    /// Handles the message asynchronously.
    /// </summary>
    /// <param name="message">The message to handle.</param>
    /// <param name="context">The message context containing metadata such as message ID, correlation ID, headers, and delivery count.</param>
    /// <param name="cancellationToken">Cancellation token that is triggered when the consumer is stopping or processing timeout is reached.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="Exception">Any exception thrown will cause the message to be requeued or dead-lettered based on configuration.</exception>
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

