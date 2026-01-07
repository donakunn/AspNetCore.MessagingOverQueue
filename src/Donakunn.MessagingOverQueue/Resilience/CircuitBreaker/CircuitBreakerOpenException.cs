namespace MessagingOverQueue.src.Resilience.CircuitBreaker;

/// <summary>
/// Exception thrown when circuit breaker is open.
/// </summary>
public class CircuitBreakerOpenException(string message, Exception? innerException = null) : Exception(message, innerException)
{
}