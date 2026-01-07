using MessagingOverQueue.src.Abstractions.Messages;
using MessagingOverQueue.src.Abstractions.Publishing;
using Microsoft.Extensions.Logging;

namespace MessagingOverQueue.src.Resilience;

/// <summary>
/// Interface for handling dead letter messages.
/// </summary>
public interface IDeadLetterHandler
{
    /// <summary>
    /// Handles a message that failed processing.
    /// </summary>
    Task HandleAsync(IMessage message, Exception exception, int retryCount, CancellationToken cancellationToken = default);
}

/// <summary>
/// Default dead letter handler that publishes to a dead letter exchange.
/// </summary>
public class DeadLetterHandler(
    IMessagePublisher publisher,
    ILogger<DeadLetterHandler> logger,
    string deadLetterExchange = "dead-letter-exchange") : IDeadLetterHandler
{
    public async Task HandleAsync(IMessage message, Exception exception, int retryCount, CancellationToken cancellationToken = default)
    {
        logger.LogWarning(exception,
            "Moving message {MessageId} to dead letter exchange after {RetryCount} retries",
            message.Id, retryCount);

        var options = new PublishOptions
        {
            ExchangeName = deadLetterExchange,
            RoutingKey = message.MessageType,
            Headers = new Dictionary<string, object>
            {
                ["x-death-reason"] = exception.Message,
                ["x-death-count"] = retryCount,
                ["x-original-message-id"] = message.Id.ToString(),
                ["x-death-timestamp"] = DateTime.UtcNow.ToString("O")
            }
        };

        try
        {
            await publisher.PublishAsync(message, options, cancellationToken);
            logger.LogInformation("Message {MessageId} moved to dead letter exchange", message.Id);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to move message {MessageId} to dead letter exchange", message.Id);
            throw;
        }
    }
}

