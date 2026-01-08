using Polly.CircuitBreaker;

namespace Donakunn.MessagingOverQueue.Resilience.CircuitBreaker;

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
    /// Gets the current circuit state (thread-safe).
    /// </summary>
    CircuitState State { get; }

    /// <summary>
    /// Event raised when circuit state changes.
    /// </summary>
    event EventHandler<CircuitStateChangedEventArgs>? StateChanged;
}
