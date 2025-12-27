using System.Text.Json;
using MessagingOverQueue.Abstractions.Publishing;
using MessagingOverQueue.Configuration.Options;
using MessagingOverQueue.Persistence.Entities;
using MessagingOverQueue.Persistence.Repositories;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MessagingOverQueue.Persistence;

/// <summary>
/// Background service that processes messages from the outbox.
/// </summary>
public class OutboxProcessor : BackgroundService
{
    private readonly IOutboxRepository _repository;
    private readonly IMessagePublisher _publisher;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxProcessor> _logger;

    public OutboxProcessor(
        IOutboxRepository repository,
        IMessagePublisher publisher,
        IOptions<OutboxOptions> options,
        ILogger<OutboxProcessor> logger)
    {
        _repository = repository;
        _publisher = publisher;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Outbox processor is disabled");
            return;
        }

        _logger.LogInformation("Outbox processor started with interval {Interval}ms", 
            _options.ProcessingInterval.TotalMilliseconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessBatchAsync(stoppingToken);
                
                if (_options.AutoCleanup)
                {
                    await CleanupAsync(stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing outbox batch");
            }

            try
            {
                await Task.Delay(_options.ProcessingInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        _logger.LogInformation("Outbox processor stopped");
    }

    private async Task ProcessBatchAsync(CancellationToken cancellationToken)
    {
        var messages = await _repository.AcquireLockAsync(
            _options.BatchSize, 
            _options.LockDuration, 
            cancellationToken);

        if (messages.Count == 0)
            return;

        _logger.LogDebug("Processing {Count} outbox messages", messages.Count);

        foreach (var message in messages)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            await ProcessMessageAsync(message, cancellationToken);
        }

        await _repository.SaveChangesAsync(cancellationToken);
    }

    private async Task ProcessMessageAsync(OutboxMessage message, CancellationToken cancellationToken)
    {
        try
        {
            if (message.RetryCount >= _options.MaxRetryAttempts)
            {
                _logger.LogWarning("Message {MessageId} exceeded max retry attempts, marking as failed", message.Id);
                await _repository.MarkAsFailedAsync(message.Id, "Max retry attempts exceeded", cancellationToken);
                return;
            }

            var options = new PublishOptions
            {
                ExchangeName = message.ExchangeName,
                RoutingKey = message.RoutingKey,
                Headers = message.Headers != null 
                    ? JsonSerializer.Deserialize<Dictionary<string, object>>(message.Headers) 
                    : null
            };

            // Use the internal direct publisher, not the outbox publisher
            var directPublisher = _publisher as Publishing.RabbitMqPublisher;
            if (directPublisher != null)
            {
                // We need to publish raw - create a wrapper for this
                await PublishRawAsync(message, options, cancellationToken);
            }

            await _repository.MarkAsPublishedAsync(message.Id, cancellationToken);
            _logger.LogDebug("Published outbox message {MessageId}", message.Id);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error publishing outbox message {MessageId}", message.Id);
            await _repository.MarkAsFailedAsync(message.Id, ex.Message, cancellationToken);
        }
    }

    private async Task PublishRawAsync(OutboxMessage message, PublishOptions options, CancellationToken cancellationToken)
    {
        // This is a simplified implementation - in production, you'd want direct channel access
        // For now, we use reflection to access the internal publish method
        var publisherType = _publisher.GetType();
        var method = publisherType.GetMethod("PublishToRabbitMqAsync", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        if (method != null)
        {
            // Create a minimal context for raw publishing
            var context = new Publishing.Middleware.PublishContext
            {
                Body = message.Payload,
                ExchangeName = message.ExchangeName,
                RoutingKey = message.RoutingKey,
                Persistent = true,
                ContentType = "application/json"
            };
            
            // Parse headers
            if (message.Headers != null)
            {
                var headers = JsonSerializer.Deserialize<Dictionary<string, object>>(message.Headers);
                if (headers != null)
                {
                    foreach (var header in headers)
                    {
                        context.Headers[header.Key] = header.Value;
                    }
                }
            }
            
            context.Headers["message-type"] = message.MessageType;
            context.Headers["message-id"] = message.Id.ToString();

            await (Task)method.Invoke(_publisher, new object[] { context, cancellationToken })!;
        }
    }

    private async Task CleanupAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _repository.CleanupAsync(_options.RetentionPeriod, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during outbox cleanup");
        }
    }
}

