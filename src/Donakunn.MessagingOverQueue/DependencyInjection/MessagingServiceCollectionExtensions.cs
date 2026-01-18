using Microsoft.Extensions.DependencyInjection;

namespace Donakunn.MessagingOverQueue.DependencyInjection;

/// <summary>
/// Extension methods for the new fluent messaging API.
/// Entry point for configuring messaging services.
/// </summary>
public static class MessagingServiceCollectionExtensions
{
    /// <summary>
    /// Adds core messaging services and returns a builder for further configuration.
    /// This is the new entry point for the fluent API.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The messaging builder for further configuration.</returns>
    /// <example>
    /// <code>
    /// services.AddMessaging()
    ///     .UseRabbitMqQueues(queues => queues
    ///         .WithConnection(opts => opts.UseHost("localhost"))
    ///         .WithTopology(t => t.ScanAssemblyContaining&lt;OrderHandler&gt;()))
    ///     .UseResilience(resilience => resilience
    ///         .WithRetry(opts => opts.MaxRetryAttempts = 5)
    ///         .WithCircuitBreaker())
    ///     .UsePersistence(persistence => persistence
    ///         .WithOutbox()
    ///             .UseSqlServer(connectionString));
    /// </code>
    /// </example>
    public static IMessagingBuilder AddMessaging(this IServiceCollection services)
    {
        return new MessagingBuilder(services);
    }
}
