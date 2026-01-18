using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Resilience.CircuitBreaker;
using Microsoft.Extensions.DependencyInjection;

namespace Donakunn.MessagingOverQueue.DependencyInjection.Resilience;

/// <summary>
/// Builder interface for configuring resilience patterns (retry, circuit breaker, timeout).
/// </summary>
public interface IResilienceBuilder
{
    /// <summary>
    /// The service collection.
    /// </summary>
    IServiceCollection Services { get; }

    /// <summary>
    /// The parent messaging builder.
    /// </summary>
    IMessagingBuilder MessagingBuilder { get; }

    /// <summary>
    /// Configures retry behavior for message processing.
    /// </summary>
    /// <param name="configure">Configuration action for retry options.</param>
    /// <returns>The builder for chaining.</returns>
    IResilienceBuilder WithRetry(Action<RetryOptions>? configure = null);

    /// <summary>
    /// Configures circuit breaker for message processing.
    /// </summary>
    /// <param name="configure">Configuration action for circuit breaker options.</param>
    /// <returns>The builder for chaining.</returns>
    IResilienceBuilder WithCircuitBreaker(Action<CircuitBreakerOptions>? configure = null);

    /// <summary>
    /// Configures timeout for message processing.
    /// </summary>
    /// <param name="timeout">The timeout duration.</param>
    /// <returns>The builder for chaining.</returns>
    IResilienceBuilder WithTimeout(TimeSpan timeout);
}
