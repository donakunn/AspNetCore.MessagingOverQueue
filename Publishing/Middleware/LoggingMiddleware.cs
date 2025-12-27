using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace MessagingOverQueue.Publishing.Middleware;

/// <summary>
/// Middleware that logs publishing operations.
/// </summary>
public class LoggingMiddleware : IPublishMiddleware
{
    private readonly ILogger<LoggingMiddleware> _logger;

    public LoggingMiddleware(ILogger<LoggingMiddleware> logger)
    {
        _logger = logger;
    }

    public async Task InvokeAsync(PublishContext context, Func<PublishContext, CancellationToken, Task> next, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        
        _logger.LogDebug("Publishing message {MessageId} to exchange '{Exchange}' with routing key '{RoutingKey}'",
            context.Message.Id, context.ExchangeName ?? "(default)", context.RoutingKey ?? "(none)");

        try
        {
            await next(context, cancellationToken);
            
            stopwatch.Stop();
            _logger.LogInformation("Published message {MessageId} to exchange '{Exchange}' in {ElapsedMs}ms",
                context.Message.Id, context.ExchangeName ?? "(default)", stopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Failed to publish message {MessageId} after {ElapsedMs}ms",
                context.Message.Id, stopwatch.ElapsedMilliseconds);
            throw;
        }
    }
}

