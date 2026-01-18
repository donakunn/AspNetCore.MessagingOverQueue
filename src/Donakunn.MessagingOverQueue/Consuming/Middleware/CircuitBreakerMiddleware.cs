using Donakunn.MessagingOverQueue.Resilience.CircuitBreaker;
using Microsoft.Extensions.Logging;
using Polly.CircuitBreaker;

namespace Donakunn.MessagingOverQueue.Consuming.Middleware;

/// <summary>
/// Middleware that wraps message processing with circuit breaker protection.
/// When the circuit is open, messages are rejected with requeue to allow processing later.
/// </summary>
public class CircuitBreakerMiddleware : IOrderedConsumeMiddleware
{
    private readonly ICircuitBreaker _circuitBreaker;
    private readonly ILogger<CircuitBreakerMiddleware> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="CircuitBreakerMiddleware"/> class.
    /// </summary>
    /// <param name="circuitBreaker">The circuit breaker instance.</param>
    /// <param name="logger">The logger.</param>
    public CircuitBreakerMiddleware(
        ICircuitBreaker circuitBreaker,
        ILogger<CircuitBreakerMiddleware> logger)
    {
        _circuitBreaker = circuitBreaker;
        _logger = logger;
    }

    /// <inheritdoc />
    public int Order => MiddlewareOrder.CircuitBreaker;

    /// <inheritdoc />
    public async Task InvokeAsync(
        ConsumeContext context,
        Func<ConsumeContext, CancellationToken, Task> next,
        CancellationToken cancellationToken)
    {
        // Check if circuit is open before attempting processing
        if (_circuitBreaker.State == CircuitState.Open)
        {
            _logger.LogWarning(
                "Circuit breaker is open, rejecting message {DeliveryTag} for requeue",
                context.DeliveryTag);

            // Requeue the message to be processed later when circuit closes
            context.ShouldReject = true;
            context.RequeueOnReject = true;
            return;
        }

        try
        {
            await _circuitBreaker.ExecuteAsync(async ct =>
            {
                await next(context, ct);

                // If the handler set an exception, throw it to trigger circuit breaker
                if (context.Exception != null)
                {
                    throw context.Exception;
                }
            }, cancellationToken);
        }
        catch (BrokenCircuitException ex)
        {
            _logger.LogWarning(
                ex,
                "Circuit breaker opened during message processing, delivery tag: {DeliveryTag}",
                context.DeliveryTag);

            context.Exception = ex;
            context.ShouldReject = true;
            context.RequeueOnReject = true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Exception was thrown but circuit breaker recorded it
            // Don't swallow - let it propagate for proper error handling
            _logger.LogError(
                ex,
                "Message processing failed with circuit breaker, delivery tag: {DeliveryTag}",
                context.DeliveryTag);

            context.Exception = ex;
            throw;
        }
    }
}
