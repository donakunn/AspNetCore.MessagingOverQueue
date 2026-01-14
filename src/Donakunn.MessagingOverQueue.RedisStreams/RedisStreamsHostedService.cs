using Donakunn.MessagingOverQueue.RedisStreams.Connection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Donakunn.MessagingOverQueue.RedisStreams;

/// <summary>
/// Hosted service that manages Redis connection lifecycle.
/// </summary>
internal sealed class RedisStreamsHostedService : IHostedService
{
    private readonly IRedisConnectionPool _connectionPool;
    private readonly ILogger<RedisStreamsHostedService> _logger;

    public RedisStreamsHostedService(
        IRedisConnectionPool connectionPool,
        ILogger<RedisStreamsHostedService> logger)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting Redis Streams hosted service");

        try
        {
            await _connectionPool.EnsureConnectedAsync(cancellationToken);
            _logger.LogInformation("Redis Streams connection established");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to establish Redis connection on startup");
            // Don't throw - allow the application to start and retry later
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping Redis Streams hosted service");
        return Task.CompletedTask;
    }
}
