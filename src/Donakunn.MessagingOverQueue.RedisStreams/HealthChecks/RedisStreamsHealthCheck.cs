using Donakunn.MessagingOverQueue.RedisStreams.Connection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace Donakunn.MessagingOverQueue.RedisStreams.HealthChecks;

/// <summary>
/// Health check for Redis Streams connectivity.
/// </summary>
public sealed class RedisStreamsHealthCheck : IHealthCheck
{
    private readonly IRedisConnectionPool _connectionPool;
    private readonly ILogger<RedisStreamsHealthCheck> _logger;

    public RedisStreamsHealthCheck(
        IRedisConnectionPool connectionPool,
        ILogger<RedisStreamsHealthCheck> logger)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (!_connectionPool.IsConnected)
            {
                return HealthCheckResult.Unhealthy("Redis connection is not established");
            }

            var db = _connectionPool.GetDatabase();
            
            // Ping the server to verify connectivity
            var latency = await db.PingAsync();

            var data = new Dictionary<string, object>
            {
                ["latency_ms"] = latency.TotalMilliseconds,
                ["connected"] = true
            };

            // Get server info
            try
            {
                var connection = _connectionPool.GetConnection();
                var endpoints = connection.GetEndPoints();
                
                if (endpoints.Length > 0)
                {
                    var server = connection.GetServer(endpoints[0]);
                    data["server_version"] = server.Version.ToString();
                    data["connected_clients"] = server.IsConnected ? "yes" : "no";
                }
            }
            catch
            {
                // Ignore errors getting extended info
            }

            if (latency.TotalMilliseconds > 1000)
            {
                return HealthCheckResult.Degraded(
                    $"Redis connection is slow (latency: {latency.TotalMilliseconds:F1}ms)",
                    data: data);
            }

            return HealthCheckResult.Healthy(
                $"Redis connection is healthy (latency: {latency.TotalMilliseconds:F1}ms)",
                data);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Redis health check failed");
            
            return HealthCheckResult.Unhealthy(
                "Redis health check failed",
                ex,
                new Dictionary<string, object>
                {
                    ["error"] = ex.Message
                });
        }
    }
}
