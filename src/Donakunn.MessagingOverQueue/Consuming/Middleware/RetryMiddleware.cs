using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Resilience;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Donakunn.MessagingOverQueue.Consuming.Middleware;

/// <summary>
/// Middleware that applies retry logic to message processing.
/// </summary>
public class RetryMiddleware(
    IRetryPolicy retryPolicy,
    IOptions<RetryOptions> options,
    ILogger<RetryMiddleware> logger) : IConsumeMiddleware
{
    private readonly RetryOptions _options = options.Value;

    public async Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
    {
        var attempt = 0;

        try
        {
            await retryPolicy.ExecuteAsync(async ct =>
            {
                attempt++;
                if (attempt > 1)
                {
                    logger.LogDebug("Retry attempt {Attempt} for message {MessageId}",
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
            logger.LogError(ex, "Message processing failed after {Attempts} attempts", attempt);
            context.Exception = ex;
            context.ShouldReject = true;
            context.RequeueOnReject = false; // Don't requeue after max retries
        }
    }
}

