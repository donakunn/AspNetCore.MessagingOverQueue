using Polly.CircuitBreaker;

namespace Donakunn.MessagingOverQueue.Resilience.CircuitBreaker;

/// <summary>
/// Event arguments for circuit breaker state changes.
/// </summary>
public class CircuitStateChangedEventArgs(CircuitState previousState, CircuitState newState, Exception? exception = null) : EventArgs
{
    public CircuitState PreviousState { get; } = previousState;
    public CircuitState NewState { get; } = newState;
    public Exception? Exception { get; } = exception;
    public DateTime Timestamp { get; } = DateTime.UtcNow;
}
