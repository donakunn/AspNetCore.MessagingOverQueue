using MessagingOverQueue.Configuration.Options;
using MessagingOverQueue.Resilience;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MessagingOverQueue.Consuming.Middleware;

/// <summary>
/// Middleware that applies retry logic to message processing.
/// </summary>
public class RetryMiddleware : IConsumeMiddleware
{
    private readonly IRetryPolicy _retryPolicy;
    private readonly RetryOptions _options;
    private readonly ILogger<RetryMiddleware> _logger;

    public RetryMiddleware(
        IRetryPolicy retryPolicy,
        IOptions<RetryOptions> options,
        ILogger<RetryMiddleware> logger)
    {
        _retryPolicy = retryPolicy;
        _options = options.Value;
        _logger = logger;
    }

    public async Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
    {
        var attempt = 0;
        
        try
        {
            await _retryPolicy.ExecuteAsync(async ct =>
            {
                attempt++;
                if (attempt > 1)
                {
                    _logger.LogDebug("Retry attempt {Attempt} for message {MessageId}",
                        attempt, context.Message?.Id);
                }
                
                await next(context, ct);
                
                if (context.Exception != null)
                {
                    throw context.Exception;
                }
            }, cancellationToken);
        }
        catch (Exception ex) when (attempt >= _options.MaxRetryAttempts)
        {
            _logger.LogError(ex, "Message processing failed after {Attempts} attempts", attempt);
            context.Exception = ex;
            context.ShouldReject = true;
            context.RequeueOnReject = false; // Don't requeue after max retries
        }
    }
}

