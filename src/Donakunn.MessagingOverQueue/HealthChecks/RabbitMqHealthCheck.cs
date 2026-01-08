using Donakunn.MessagingOverQueue.Connection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace Donakunn.MessagingOverQueue.HealthChecks;

/// <summary>
/// Health check for RabbitMQ connection.
/// </summary>
public class RabbitMqHealthCheck : IHealthCheck
{
    private readonly IRabbitMqConnectionPool _connectionPool;
    private readonly ILogger<RabbitMqHealthCheck> _logger;

    public RabbitMqHealthCheck(
        IRabbitMqConnectionPool connectionPool,
        ILogger<RabbitMqHealthCheck> logger)
    {
        _connectionPool = connectionPool;
        _logger = logger;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (!_connectionPool.IsConnected)
            {
                await _connectionPool.EnsureConnectedAsync(cancellationToken);
            }

            if (_connectionPool.IsConnected)
            {
                // Try to get a channel to verify the connection is fully functional
                var channel = await _connectionPool.GetChannelAsync(cancellationToken);
                _connectionPool.ReturnChannel(channel);

                return HealthCheckResult.Healthy("RabbitMQ connection is healthy");
            }

            return HealthCheckResult.Unhealthy("RabbitMQ connection is not open");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RabbitMQ health check failed");
            return HealthCheckResult.Unhealthy(
                "RabbitMQ health check failed",
                ex,
                new Dictionary<string, object>
                {
                    ["ExceptionType"] = ex.GetType().Name,
                    ["Message"] = ex.Message
                });
        }
    }
}

