using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Persistence.Entities;
using Donakunn.MessagingOverQueue.Persistence.Providers;
using Donakunn.MessagingOverQueue.Persistence.Repositories;
using Donakunn.MessagingOverQueue.Providers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace Donakunn.MessagingOverQueue.Persistence;

/// <summary>
/// Background service that processes messages from the outbox.
/// </summary>
public sealed class OutboxProcessor : BackgroundService
{
    private readonly IOutboxRepository _repository;
    private readonly IInboxRepository _inboxRepository;
    private readonly IMessageStoreProvider _provider;
    private readonly IInternalPublisher _internalPublisher;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxProcessor> _logger;

    private DateTime _lastCleanupTime = DateTime.MinValue;

    public OutboxProcessor(
        IOutboxRepository repository,
        IInboxRepository inboxRepository,
        IMessageStoreProvider provider,
        IInternalPublisher internalPublisher,
        IOptions<OutboxOptions> options,
        ILogger<OutboxProcessor> logger)
    {
        _repository = repository;
        _inboxRepository = inboxRepository;
        _provider = provider;
        _internalPublisher = internalPublisher;
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

        // Ensure schema exists on startup
        if (_options.AutoCreateSchema)
        {
            try
            {
                await _provider.EnsureSchemaAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to ensure message store schema");
                throw;
            }
        }

        _logger.LogInformation("Outbox processor started with interval {Interval}ms",
            _options.ProcessingInterval.TotalMilliseconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessBatchAsync(stoppingToken);

                if (_options.AutoCleanup && ShouldRunCleanup())
                {
                    await CleanupAsync(stoppingToken);
                    _lastCleanupTime = DateTime.UtcNow;
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

    private bool ShouldRunCleanup()
    {
        return DateTime.UtcNow - _lastCleanupTime >= _options.CleanupInterval;
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
    }

    private async Task ProcessMessageAsync(MessageStoreEntry message, CancellationToken cancellationToken)
    {
        try
        {
            if (message.RetryCount >= _options.MaxRetryAttempts)
            {
                _logger.LogWarning("Message {MessageId} exceeded max retry attempts, marking as failed", message.Id);
                await _repository.MarkAsFailedAsync(message.Id, "Max retry attempts exceeded", cancellationToken);
                return;
            }

            await PublishRawAsync(message, cancellationToken);

            await _repository.MarkAsPublishedAsync(message.Id, cancellationToken);
            _logger.LogDebug("Published outbox message {MessageId}", message.Id);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error publishing outbox message {MessageId}", message.Id);
            await _repository.MarkAsFailedAsync(message.Id, ex.Message, cancellationToken);
        }
    }

    private async Task PublishRawAsync(MessageStoreEntry message, CancellationToken cancellationToken)
    {
        if (message.Payload == null || message.Payload.Length == 0)
            throw new ArgumentException($"Outbox message {message.Id} has empty payload.");

        var context = new Publishing.Middleware.PublishContext
        {
            Body = message.Payload,
            ExchangeName = message.ExchangeName,
            RoutingKey = message.RoutingKey,
            QueueName = message.RoutingKey, // For Redis Streams, use routing key as queue/stream name
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
                _logger.LogWarning(ex, "Failed to deserialize headers for outbox message {MessageId}", message.Id);
            }
        }

        context.Headers["message-type"] = message.MessageType;
        context.Headers["message-id"] = message.Id.ToString();

        await _internalPublisher.PublishAsync(context, cancellationToken);
    }

    private async Task CleanupAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _inboxRepository.CleanupAsync(_options.RetentionPeriod, cancellationToken);
            await _repository.CleanupAsync(_options.RetentionPeriod, cancellationToken);

            _logger.LogDebug("Completed message store cleanup");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during message store cleanup");
        }
    }
}

