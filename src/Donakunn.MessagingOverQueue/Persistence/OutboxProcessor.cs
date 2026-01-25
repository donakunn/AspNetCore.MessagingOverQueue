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
/// Supports multiple instances for horizontal scaling with partitioned message processing.
/// </summary>
public sealed class OutboxProcessor : BackgroundService
{
    private readonly IOutboxRepository _repository;
    private readonly IInboxRepository _inboxRepository;
    private readonly IMessageStoreProvider _provider;
    private readonly IInternalPublisher _internalPublisher;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxProcessor> _logger;
    private readonly int _workerId;
    private readonly int[] _assignedPartitions;

    private DateTime _lastCleanupTime = DateTime.MinValue;

    /// <summary>
    /// Creates a new OutboxProcessor instance.
    /// </summary>
    /// <param name="repository">The outbox repository.</param>
    /// <param name="inboxRepository">The inbox repository for cleanup.</param>
    /// <param name="provider">The message store provider.</param>
    /// <param name="internalPublisher">The internal publisher for sending messages.</param>
    /// <param name="options">Outbox configuration options.</param>
    /// <param name="logger">Logger instance.</param>
    /// <param name="workerId">The worker ID (0-based). Defaults to 0 for single worker scenarios.</param>
    public OutboxProcessor(
        IOutboxRepository repository,
        IInboxRepository inboxRepository,
        IMessageStoreProvider provider,
        IInternalPublisher internalPublisher,
        IOptions<OutboxOptions> options,
        ILogger<OutboxProcessor> logger,
        int workerId = 0)
    {
        _repository = repository;
        _inboxRepository = inboxRepository;
        _provider = provider;
        _internalPublisher = internalPublisher;
        _options = options.Value;
        _logger = logger;
        _workerId = workerId;

        // Calculate assigned partitions for this worker
        // Worker 0 of 3 with 6 partitions gets: [0, 3]
        // Worker 1 of 3 with 6 partitions gets: [1, 4]
        // Worker 2 of 3 with 6 partitions gets: [2, 5]
        _assignedPartitions = Enumerable.Range(0, _options.PartitionCount)
            .Where(p => p % _options.WorkerCount == _workerId)
            .ToArray();
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
                await _provider.EnsureSchemaAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to ensure message store schema");
                throw;
            }
        }

        _logger.LogInformation(
            "Outbox processor worker {WorkerId} started with interval {Interval}ms, assigned partitions: [{Partitions}]",
            _workerId, _options.ProcessingInterval.TotalMilliseconds, string.Join(",", _assignedPartitions));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessBatchAsync(stoppingToken).ConfigureAwait(false);

                if (_options.AutoCleanup && ShouldRunCleanup())
                {
                    await CleanupAsync(stoppingToken).ConfigureAwait(false);
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
                await Task.Delay(_options.ProcessingInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        _logger.LogInformation("Outbox processor worker {WorkerId} stopped", _workerId);
    }

    private bool ShouldRunCleanup()
    {
        return DateTime.UtcNow - _lastCleanupTime >= _options.CleanupInterval;
    }

    private async Task ProcessBatchAsync(CancellationToken cancellationToken)
    {
        // Acquire lock on messages for this worker's assigned partitions
        var messages = await _repository.AcquireLockAsync(
            _options.BatchSize,
            _options.LockDuration,
            _assignedPartitions,
            _options.PartitionCount,
            cancellationToken).ConfigureAwait(false);

        if (messages.Count == 0)
            return;

        _logger.LogDebug("Worker {WorkerId} processing {Count} outbox messages", _workerId, messages.Count);

        // Filter out messages that exceeded max retry attempts
        var messagesToProcess = new List<MessageStoreEntry>(messages.Count);
        var messagesToFail = new List<(Guid Id, string Error)>(Math.Min(messages.Count / 10 + 1, 10));

        foreach (var message in messages)
        {
            if (message.RetryCount >= _options.MaxRetryAttempts)
            {
                _logger.LogWarning("Message {MessageId} exceeded max retry attempts, marking as failed", message.Id);
                messagesToFail.Add((message.Id, "Max retry attempts exceeded"));
            }
            else
            {
                messagesToProcess.Add(message);
            }
        }

        // Mark exceeded retry messages as failed
        if (messagesToFail.Count > 0)
        {
            await _repository.MarkAsFailedBatchAsync(messagesToFail, cancellationToken).ConfigureAwait(false);
        }

        if (messagesToProcess.Count == 0)
            return;

        // Process in publish batch chunks
        var publishBatches = messagesToProcess.Chunk(_options.PublishBatchSize);

        foreach (var batch in publishBatches)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            await ProcessPublishBatchAsync(batch, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ProcessPublishBatchAsync(MessageStoreEntry[] batch, CancellationToken cancellationToken)
    {
        // Build publish contexts for the batch
        var contexts = new List<(Guid Id, Publishing.Middleware.PublishContext Context)>(batch.Length);
        foreach (var message in batch)
        {
            try
            {
                var context = BuildPublishContext(message);
                contexts.Add((message.Id, context));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error building publish context for message {MessageId}", message.Id);
                await _repository.MarkAsFailedAsync(message.Id, ex.Message, cancellationToken).ConfigureAwait(false);
            }
        }

        if (contexts.Count == 0)
            return;

        // Publish batch - returns individual results for partial success
        var publishResults = await _internalPublisher.PublishBatchAsync(
            contexts.Select(c => c.Context).ToList(),
            cancellationToken).ConfigureAwait(false);

        // Map results back to message IDs
        var succeeded = new List<Guid>(contexts.Count);
        var failed = new List<(Guid Id, string Error)>(Math.Min(contexts.Count / 10 + 1, 10));

        for (int i = 0; i < publishResults.Count; i++)
        {
            var result = publishResults[i];
            var messageId = contexts[i].Id;

            if (result.Success)
            {
                succeeded.Add(messageId);
                _logger.LogDebug("Published outbox message {MessageId}", messageId);
            }
            else
            {
                failed.Add((messageId, result.Error ?? "Unknown error"));
                _logger.LogError("Failed to publish outbox message {MessageId}: {Error}", messageId, result.Error);
            }
        }

        // Batch update statuses in parallel
        var updateTasks = new List<Task>(2);
        if (succeeded.Count > 0)
            updateTasks.Add(_repository.MarkAsPublishedBatchAsync(succeeded, cancellationToken));
        if (failed.Count > 0)
            updateTasks.Add(_repository.MarkAsFailedBatchAsync(failed, cancellationToken));

        if (updateTasks.Count > 0)
            await Task.WhenAll(updateTasks).ConfigureAwait(false);
    }

    private Publishing.Middleware.PublishContext BuildPublishContext(MessageStoreEntry message)
    {
        if (message.Payload == null || message.Payload.Length == 0)
            throw new ArgumentException($"Outbox message {message.Id} has empty payload.");

        var context = new Publishing.Middleware.PublishContext
        {
            Body = message.Payload,
            ExchangeName = message.ExchangeName,
            RoutingKey = message.RoutingKey,
            QueueName = message.QueueName ?? message.RoutingKey,
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

        return context;
    }

    private async Task CleanupAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _inboxRepository.CleanupAsync(_options.RetentionPeriod, cancellationToken).ConfigureAwait(false);
            await _repository.CleanupAsync(_options.RetentionPeriod, cancellationToken).ConfigureAwait(false);

            _logger.LogDebug("Completed message store cleanup");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during message store cleanup");
        }
    }
}

