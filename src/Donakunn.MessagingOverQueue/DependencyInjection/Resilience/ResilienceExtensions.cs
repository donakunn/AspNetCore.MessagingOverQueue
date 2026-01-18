namespace Donakunn.MessagingOverQueue.DependencyInjection.Resilience;

/// <summary>
/// Extension methods for configuring resilience patterns.
/// </summary>
public static class ResilienceExtensions
{
    /// <summary>
    /// Configures resilience patterns (retry, circuit breaker, timeout) for message processing.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="configure">Configuration action for the resilience builder.</param>
    /// <returns>The messaging builder for chaining.</returns>
    /// <example>
    /// <code>
    /// services.AddMessaging()
    ///     .UseRabbitMqQueues(...)
    ///     .UseResilience(resilience => resilience
    ///         .WithRetry(opts => opts.MaxRetryAttempts = 5)
    ///         .WithCircuitBreaker(opts => opts.FailureThreshold = 10)
    ///         .WithTimeout(TimeSpan.FromSeconds(30)));
    /// </code>
    /// </example>
    public static IMessagingBuilder UseResilience(
        this IMessagingBuilder builder,
        Action<IResilienceBuilder> configure)
    {
        var resilienceBuilder = new ResilienceBuilder(builder.Services, builder);
        configure(resilienceBuilder);
        return builder;
    }
}
