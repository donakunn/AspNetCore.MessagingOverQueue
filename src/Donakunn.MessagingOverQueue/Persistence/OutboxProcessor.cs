using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Persistence.Entities;
using Donakunn.MessagingOverQueue.Persistence.Repositories;
using Donakunn.MessagingOverQueue.Publishing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace Donakunn.MessagingOverQueue.Persistence;

/// <summary>
/// Background service that processes messages from the outbox.
/// </summary>
public class OutboxProcessor(
    IServiceScopeFactory scopeFactory,
    IMessagePublisher publisher,
    IOptions<OutboxOptions> options,
    ILogger<OutboxProcessor> logger) : BackgroundService
{
    private readonly OutboxOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            logger.LogInformation("Outbox processor is disabled");
            return;
        }

        logger.LogInformation("Outbox processor started with interval {Interval}ms",
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
                logger.LogError(ex, "Error processing outbox batch");
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

        logger.LogInformation("Outbox processor stopped");
    }

    private async Task ProcessBatchAsync(CancellationToken cancellationToken)
    {
        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        var messages = await repository.AcquireLockAsync(
            _options.BatchSize,
            _options.LockDuration,
            cancellationToken);

        if (messages.Count == 0)
            return;

        logger.LogDebug("Processing {Count} outbox messages", messages.Count);

        foreach (var message in messages)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            await ProcessMessageAsync(repository, message, cancellationToken);
        }

        await repository.SaveChangesAsync(cancellationToken);
    }

    private async Task ProcessMessageAsync(IOutboxRepository repository, OutboxMessage message, CancellationToken cancellationToken)
    {
        try
        {
            if (message.RetryCount >= _options.MaxRetryAttempts)
            {
                logger.LogWarning("Message {MessageId} exceeded max retry attempts, marking as failed", message.Id);
                await repository.MarkAsFailedAsync(message.Id, "Max retry attempts exceeded", cancellationToken);
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
            var directPublisher = (((publisher as RabbitMqPublisher)));
            if (directPublisher != null)
            {
                // We need to publish raw - create a wrapper for this
                await PublishRawAsync(message, options, cancellationToken);
            }

            await repository.MarkAsPublishedAsync(message.Id, cancellationToken);
            logger.LogDebug("Published outbox message {MessageId}", message.Id);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error publishing outbox message {MessageId}", message.Id);
            await repository.MarkAsFailedAsync(message.Id, ex.Message, cancellationToken);
        }
    }

    private async Task PublishRawAsync(OutboxMessage message, PublishOptions options, CancellationToken cancellationToken)
    {
        if (message.Payload == null || message.Payload.Length == 0)
            throw new ArgumentException($"Outbox message {message.Id} has empty payload.");

        var context = new Publishing.Middleware.PublishContext
        {
            Body = message.Payload,
            ExchangeName = message.ExchangeName,
            RoutingKey = message.RoutingKey,
            Persistent = true,
            ContentType = "application/json"
        };

        // Parse headers
        if (!string.IsNullOrWhiteSpace(message.Headers))
        {
            try
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
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to deserialize headers for outbox message {MessageId}", message.Id);
            }
        }

        context.Headers["message-type"] = message.MessageType;
        context.Headers["message-id"] = message.Id.ToString();

        if (publisher is RabbitMqPublisher directPublisher)
        {

            await directPublisher.PublishToRabbitMqAsync(context, cancellationToken);
        }
        else
        {
            throw new InvalidOperationException("Publisher does not support raw publishing.");
        }
    }

    private async Task CleanupAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var scope = scopeFactory.CreateScope();

            var outBoxRepository = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();
            var inboxRepository = scope.ServiceProvider.GetRequiredService<IInboxRepository>();

            await inboxRepository.CleanupAsync(_options.RetentionPeriod, cancellationToken);
            await outBoxRepository.CleanupAsync(_options.RetentionPeriod, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Error during outbox cleanup");
        }
    }
}

