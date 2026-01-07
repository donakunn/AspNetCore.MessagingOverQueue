using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace MessagingOverQueue.src.Consuming.Middleware;

/// <summary>
/// Middleware that logs consume operations.
/// </summary>
public class ConsumeLoggingMiddleware : IConsumeMiddleware
{
    private readonly ILogger<ConsumeLoggingMiddleware> _logger;

    public ConsumeLoggingMiddleware(ILogger<ConsumeLoggingMiddleware> logger)
    {
        _logger = logger;
    }

    public async Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        _logger.LogDebug("Processing message from queue '{Queue}', delivery tag: {DeliveryTag}, redelivered: {Redelivered}",
            context.MessageContext.QueueName, context.DeliveryTag, context.Redelivered);

        try
        {
            await next(context, cancellationToken);

            stopwatch.Stop();

            if (context.Message != null)
            {
                _logger.LogInformation("Processed message {MessageId} of type {MessageType} in {ElapsedMs}ms",
                    context.Message.Id, context.MessageType?.Name, stopwatch.ElapsedMilliseconds);
            }
            else
            {
                _logger.LogDebug("Processed delivery tag {DeliveryTag} in {ElapsedMs}ms",
                    context.DeliveryTag, stopwatch.ElapsedMilliseconds);
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error processing message after {ElapsedMs}ms, delivery tag: {DeliveryTag}",
                stopwatch.ElapsedMilliseconds, context.DeliveryTag);
            context.Exception = ex;
            throw;
        }
    }
}

