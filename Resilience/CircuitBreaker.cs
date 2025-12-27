using Polly;
using Polly.CircuitBreaker;

namespace MessagingOverQueue.Resilience;

/// <summary>
/// Configuration for circuit breaker.
/// </summary>
public class CircuitBreakerOptions
{
    /// <summary>
    /// Number of failures before opening the circuit.
    /// </summary>
    public int FailureThreshold { get; set; } = 5;
    
    /// <summary>
    /// Duration the circuit stays open.
    /// </summary>
    public TimeSpan DurationOfBreak { get; set; } = TimeSpan.FromSeconds(30);
    
    /// <summary>
    /// Sampling duration for failure counting.
    /// </summary>
    public TimeSpan SamplingDuration { get; set; } = TimeSpan.FromMinutes(1);
    
    /// <summary>
    /// Minimum throughput before circuit can open.
    /// </summary>
    public int MinimumThroughput { get; set; } = 10;
    
    /// <summary>
    /// Failure rate threshold (0.0 to 1.0).
    /// </summary>
    public double FailureRateThreshold { get; set; } = 0.5;
}

/// <summary>
/// Interface for circuit breaker functionality.
/// </summary>
public interface ICircuitBreaker
{
    /// <summary>
    /// Executes an action with circuit breaker protection.
    /// </summary>
    Task ExecuteAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Executes a function with circuit breaker protection.
    /// </summary>
    Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets the current circuit state.
    /// </summary>
    CircuitState State { get; }
}

/// <summary>
/// Polly-based circuit breaker implementation using Polly v8.
/// </summary>
public class PollyCircuitBreaker : ICircuitBreaker
{
    private readonly ResiliencePipeline _pipeline;
    private CircuitState _currentState = CircuitState.Closed;

    public PollyCircuitBreaker(CircuitBreakerOptions options)
    {
        _pipeline = new ResiliencePipelineBuilder()
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                FailureRatio = options.FailureRateThreshold,
                SamplingDuration = options.SamplingDuration,
                MinimumThroughput = options.MinimumThroughput,
                BreakDuration = options.DurationOfBreak,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(),
                OnOpened = args =>
                {
                    _currentState = CircuitState.Open;
                    return ValueTask.CompletedTask;
                },
                OnClosed = args =>
                {
                    _currentState = CircuitState.Closed;
                    return ValueTask.CompletedTask;
                },
                OnHalfOpened = args =>
                {
                    _currentState = CircuitState.HalfOpen;
                    return ValueTask.CompletedTask;
                }
            })
            .Build();
    }

    public CircuitState State => _currentState;

    public async Task ExecuteAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken = default)
    {
        await _pipeline.ExecuteAsync(async ct => await action(ct), cancellationToken);
    }

    public async Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken = default)
    {
        return await _pipeline.ExecuteAsync(async ct => await action(ct), cancellationToken);
    }
}

/// <summary>
/// Represents the state of a circuit breaker.
/// </summary>
public enum CircuitState
{
    /// <summary>
    /// Circuit is closed and allowing requests.
    /// </summary>
    Closed,
    
    /// <summary>
    /// Circuit is open and blocking requests.
    /// </summary>
    Open,
    
    /// <summary>
    /// Circuit is half-open and testing requests.
    /// </summary>
    HalfOpen
}
