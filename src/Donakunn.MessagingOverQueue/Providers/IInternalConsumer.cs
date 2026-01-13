using Donakunn.MessagingOverQueue.Consuming.Middleware;

namespace Donakunn.MessagingOverQueue.Providers;

/// <summary>
/// Internal consumer abstraction for receiving messages from a broker.
/// This interface is implemented by provider-specific consumers (RabbitMQ, Redis Streams, etc.)
/// and is used by the hosted service to process messages through the middleware pipeline.
/// </summary>
public interface IInternalConsumer : IAsyncDisposable
{
    /// <summary>
    /// Gets whether the consumer is currently running.
    /// </summary>
    bool IsRunning { get; }

    /// <summary>
    /// Gets the queue or stream name this consumer is receiving from.
    /// </summary>
    string SourceName { get; }

    /// <summary>
    /// Starts consuming messages from the broker.
    /// </summary>
    /// <param name="handler">The handler to invoke for each received message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <remarks>
    /// The handler receives a ConsumeContext with:
    /// - Body: The raw message bytes
    /// - Headers: Message metadata
    /// - MessageContext: Contextual information about the delivery
    /// 
    /// The handler is responsible for:
    /// - Deserialization
    /// - Handler invocation
    /// - Setting acknowledgment flags on the context
    /// </remarks>
    Task StartAsync(Func<ConsumeContext, CancellationToken, Task> handler, CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops consuming messages gracefully.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task StopAsync(CancellationToken cancellationToken = default);
}
