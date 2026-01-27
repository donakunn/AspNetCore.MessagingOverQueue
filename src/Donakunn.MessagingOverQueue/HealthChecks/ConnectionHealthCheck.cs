using Donakunn.MessagingOverQueue.Providers;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace Donakunn.MessagingOverQueue.HealthChecks;

/// <summary>
/// Provider-agnostic health check for messaging connectivity.
/// Delegates to the registered <see cref="IMessagingProvider"/> for provider-specific health checks.
/// </summary>
public sealed class ConnectionHealthCheck : IHealthCheck
{
    private readonly IMessagingProvider _messagingProvider;
    private readonly ILogger<ConnectionHealthCheck> _logger;

    public ConnectionHealthCheck(
        IMessagingProvider messagingProvider,
        ILogger<ConnectionHealthCheck> logger)
    {
        _messagingProvider = messagingProvider ?? throw new ArgumentNullException(nameof(messagingProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // Quick check: is the provider connected?
            if (!_messagingProvider.IsConnected)
            {
                return HealthCheckResult.Unhealthy(
                    $"{_messagingProvider.ProviderName} connection not established");
            }

            // Delegate to provider-specific health check for detailed verification
            var providerHealthCheck = _messagingProvider.CreateHealthCheck();
            var result = await providerHealthCheck.CheckHealthAsync(context, cancellationToken);

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Health check failed for {Provider}", _messagingProvider.ProviderName);
            return HealthCheckResult.Unhealthy(
                $"{_messagingProvider.ProviderName} health check failed",
                ex);
        }
    }
}
