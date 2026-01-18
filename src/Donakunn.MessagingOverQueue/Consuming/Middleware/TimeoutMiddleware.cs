using Donakunn.MessagingOverQueue.DependencyInjection.Resilience;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Donakunn.MessagingOverQueue.Consuming.Middleware;

/// <summary>
/// Middleware that enforces a maximum processing time for messages.
/// If processing exceeds the timeout, the operation is cancelled.
/// </summary>
public class TimeoutMiddleware : IOrderedConsumeMiddleware
{
    private readonly TimeoutOptions _options;
    private readonly ILogger<TimeoutMiddleware> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="TimeoutMiddleware"/> class.
    /// </summary>
    /// <param name="options">The timeout options.</param>
    /// <param name="logger">The logger.</param>
    public TimeoutMiddleware(
        IOptions<TimeoutOptions> options,
        ILogger<TimeoutMiddleware> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public int Order => MiddlewareOrder.Timeout;

    /// <inheritdoc />
    public async Task InvokeAsync(
        ConsumeContext context,
        Func<ConsumeContext, CancellationToken, Task> next,
        CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_options.Timeout);

        try
        {
            await next(context, timeoutCts.Token);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            // Timeout occurred (not external cancellation)
            var timeoutException = new TimeoutException(
                $"Message processing timed out after {_options.Timeout.TotalSeconds:F1} seconds");

            _logger.LogError(
                timeoutException,
                "Message processing timed out after {TimeoutSeconds}s, delivery tag: {DeliveryTag}",
                _options.Timeout.TotalSeconds,
                context.DeliveryTag);

            context.Exception = timeoutException;
            context.ShouldReject = true;
            context.RequeueOnReject = false; // Don't requeue timed-out messages by default

            throw timeoutException;
        }
    }
}
