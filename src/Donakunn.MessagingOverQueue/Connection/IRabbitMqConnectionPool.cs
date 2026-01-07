using RabbitMQ.Client;

namespace MessagingOverQueue.src.Connection;

/// <summary>
/// Interface for managing RabbitMQ connections and channels.
/// </summary>
public interface IRabbitMqConnectionPool : IAsyncDisposable
{
    /// <summary>
    /// Gets a channel from the pool for publishing.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A pooled channel.</returns>
    Task<IChannel> GetChannelAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns a channel to the pool.
    /// </summary>
    /// <param name="channel">The channel to return.</param>
    void ReturnChannel(IChannel channel);

    /// <summary>
    /// Creates a dedicated channel for consuming.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A new dedicated channel.</returns>
    Task<IChannel> CreateDedicatedChannelAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets whether the connection is open.
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    /// Ensures the connection is established.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task EnsureConnectedAsync(CancellationToken cancellationToken = default);
}

