using Donakunn.MessagingOverQueue.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Donakunn.MessagingOverQueue.HealthChecks;

/// <summary>
/// Extension methods for adding messaging health checks.
/// </summary>
public static class HealthCheckExtensions
{
    /// <summary>
    /// Adds connection health checks for the messaging system.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="name">The health check name.</param>
    /// <param name="failureStatus">The failure status to report when unhealthy.</param>
    /// <param name="tags">Optional tags for the health check.</param>
    /// <returns>The messaging builder for chaining.</returns>
    public static IMessagingBuilder AddHealthCheck(
        this IMessagingBuilder builder,
        string name = "messaging-connection",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
    {
        builder.Services.TryAddSingleton<ConnectionHealthCheck>();
        builder.Services.AddHealthChecks()
            .AddCheck<ConnectionHealthCheck>(
                name,
                failureStatus,
                tags ?? []);

        return builder;
    }
}
