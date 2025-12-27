using MessagingOverQueue.Abstractions.Messages;
using MessagingOverQueue.Abstractions.Publishing;
using Microsoft.Extensions.Logging;

namespace MessagingOverQueue.Resilience;

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
public class DeadLetterHandler : IDeadLetterHandler
{
    private readonly IMessagePublisher _publisher;
    private readonly ILogger<DeadLetterHandler> _logger;
    private readonly string _deadLetterExchange;

    public DeadLetterHandler(
        IMessagePublisher publisher,
        ILogger<DeadLetterHandler> logger,
        string deadLetterExchange = "dead-letter-exchange")
    {
        _publisher = publisher;
        _logger = logger;
        _deadLetterExchange = deadLetterExchange;
    }

    public async Task HandleAsync(IMessage message, Exception exception, int retryCount, CancellationToken cancellationToken = default)
    {
        _logger.LogWarning(exception, 
            "Moving message {MessageId} to dead letter exchange after {RetryCount} retries", 
            message.Id, retryCount);

        var options = new PublishOptions
        {
            ExchangeName = _deadLetterExchange,
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
            await _publisher.PublishAsync(message, options, cancellationToken);
            _logger.LogInformation("Message {MessageId} moved to dead letter exchange", message.Id);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to move message {MessageId} to dead letter exchange", message.Id);
            throw;
        }
    }
}

