using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.Resilience;
using Donakunn.MessagingOverQueue.Resilience.CircuitBreaker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Donakunn.MessagingOverQueue.DependencyInjection.Resilience;

/// <summary>
/// Implementation of the resilience builder.
/// </summary>
internal sealed class ResilienceBuilder : IResilienceBuilder
{
    public IServiceCollection Services { get; }
    public IMessagingBuilder MessagingBuilder { get; }

    public ResilienceBuilder(IServiceCollection services, IMessagingBuilder messagingBuilder)
    {
        Services = services;
        MessagingBuilder = messagingBuilder;
    }

    public IResilienceBuilder WithRetry(Action<RetryOptions>? configure = null)
    {
        Services.Configure<RetryOptions>(options =>
        {
            configure?.Invoke(options);
        });

        // Ensure retry policy is registered
        Services.TryAddSingleton<IRetryPolicy, PollyRetryPolicy>();

        // Register retry middleware
        Services.AddSingleton<RetryMiddleware>();
        Services.AddSingleton<IConsumeMiddleware>(sp => sp.GetRequiredService<RetryMiddleware>());

        return this;
    }

    public IResilienceBuilder WithCircuitBreaker(Action<CircuitBreakerOptions>? configure = null)
    {
        Services.Configure<CircuitBreakerOptions>(options =>
        {
            configure?.Invoke(options);
        });

        // Register circuit breaker using IOptions pattern for proper configuration binding
        Services.TryAddSingleton<ICircuitBreaker>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<CircuitBreakerOptions>>().Value;
            return new PollyCircuitBreaker(options);
        });

        // Register circuit breaker middleware
        Services.AddSingleton<CircuitBreakerMiddleware>();
        Services.AddSingleton<IConsumeMiddleware>(sp => sp.GetRequiredService<CircuitBreakerMiddleware>());

        return this;
    }

    public IResilienceBuilder WithTimeout(TimeSpan timeout)
    {
        Services.Configure<TimeoutOptions>(options =>
        {
            options.Timeout = timeout;
        });

        // Register timeout middleware
        Services.AddSingleton<TimeoutMiddleware>();
        Services.AddSingleton<IConsumeMiddleware>(sp => sp.GetRequiredService<TimeoutMiddleware>());

        return this;
    }
}

/// <summary>
/// Configuration options for timeout.
/// </summary>
public class TimeoutOptions
{
    /// <summary>
    /// The timeout duration for message processing.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);
}
