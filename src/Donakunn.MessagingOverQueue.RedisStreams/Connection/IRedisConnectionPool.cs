using StackExchange.Redis;

namespace Donakunn.MessagingOverQueue.RedisStreams.Connection;

/// <summary>
/// Interface for managing Redis connections.
/// </summary>
public interface IRedisConnectionPool : IAsyncDisposable
{
    /// <summary>
    /// Gets the connection multiplexer.
    /// </summary>
    IConnectionMultiplexer GetConnection();

    /// <summary>
    /// Gets a database instance for the configured database index.
    /// </summary>
    IDatabase GetDatabase();

    /// <summary>
    /// Gets whether the connection is established and healthy.
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    /// Ensures the connection is established.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task EnsureConnectedAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a subscriber for pub/sub operations.
    /// </summary>
    ISubscriber GetSubscriber();
}
