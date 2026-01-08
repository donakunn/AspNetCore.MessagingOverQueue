namespace Donakunn.MessagingOverQueue.Abstractions.Consuming;

/// <summary>
/// Interface for consuming messages from RabbitMQ.
/// </summary>
public interface IMessageConsumer : IAsyncDisposable
{
    /// <summary>
    /// Starts consuming messages.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops consuming messages.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets whether the consumer is currently running.
    /// </summary>
    bool IsRunning { get; }
}

