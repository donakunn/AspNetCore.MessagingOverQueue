using Donakunn.MessagingOverQueue.Abstractions.Consuming;
using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.Providers;
using Donakunn.MessagingOverQueue.RedisStreams.Configuration;
using Donakunn.MessagingOverQueue.RedisStreams.Connection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Collections.Concurrent;
using System.Text.Json;

namespace Donakunn.MessagingOverQueue.RedisStreams;

/// <summary>
/// Redis Streams implementation of the internal consumer.
/// Consumes messages using consumer groups with automatic claiming of idle messages.
/// Thread-safe implementation with in-flight message tracking to prevent duplicate processing.
/// </summary>
public sealed class RedisStreamsConsumer : IInternalConsumer
{
    private readonly IRedisConnectionPool _connectionPool;
    private readonly RedisStreamsOptions _options;
    private readonly ConsumerOptions _consumerOptions;
    private readonly ILogger<RedisStreamsConsumer> _logger;
    private readonly string _streamKey;
    private readonly string _consumerGroup;
    private readonly string _consumerId;
    private readonly SemaphoreSlim _concurrencySemaphore;
    private readonly CancellationTokenSource _stoppingCts = new();

    /// <summary>
    /// Tracks entry IDs currently being processed to prevent duplicate processing.
    /// Key: Redis entry ID string, Value: timestamp when processing started (for diagnostics).
    /// </summary>
    private readonly ConcurrentDictionary<string, long> _inFlightEntries = new();

    private Task? _processingTask;
    private Task? _claimingTask;
    private volatile bool _isRunning;

    public RedisStreamsConsumer(
        IRedisConnectionPool connectionPool,
        IOptions<RedisStreamsOptions> options,
        ConsumerOptions consumerOptions,
        string streamKey,
        string consumerGroup,
        ILogger<RedisStreamsConsumer> logger)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _consumerOptions = consumerOptions ?? throw new ArgumentNullException(nameof(consumerOptions));
        _streamKey = streamKey ?? throw new ArgumentNullException(nameof(streamKey));
        _consumerGroup = consumerGroup ?? throw new ArgumentNullException(nameof(consumerGroup));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _consumerId = _options.ConsumerId
            ?? $"{Environment.MachineName}-{Guid.NewGuid():N}"[..32];

        _concurrencySemaphore = new SemaphoreSlim(
            _consumerOptions.MaxConcurrency,
            _consumerOptions.MaxConcurrency);
    }

    /// <inheritdoc />
    public bool IsRunning => _isRunning;

    /// <inheritdoc />
    public string SourceName => _streamKey;

    /// <inheritdoc />
    public async Task StartAsync(
        Func<ConsumeContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken = default)
    {
        if (_isRunning)
            return;

        _logger.LogInformation(
            "Starting Redis Streams consumer for stream '{StreamKey}', group '{ConsumerGroup}', consumer '{ConsumerId}'",
            _streamKey, _consumerGroup, _consumerId);

        await EnsureConsumerGroupExistsAsync(cancellationToken);

        _isRunning = true;

        // Start the main message processing loop
        _processingTask = ProcessMessagesLoopAsync(handler, _stoppingCts.Token);

        // Start the idle message claiming loop
        _claimingTask = ClaimIdleMessagesLoopAsync(handler, _stoppingCts.Token);

        _logger.LogInformation("Redis Streams consumer started for stream '{StreamKey}'", _streamKey);
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (!_isRunning)
            return;

        _logger.LogInformation("Stopping Redis Streams consumer for stream '{StreamKey}'", _streamKey);

        _isRunning = false;
        await _stoppingCts.CancelAsync();

        // Wait for processing tasks to complete
        var tasks = new List<Task>();
        if (_processingTask != null) tasks.Add(_processingTask);
        if (_claimingTask != null) tasks.Add(_claimingTask);

        if (tasks.Count > 0)
        {
            try
            {
                await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
            catch (TimeoutException)
            {
                _logger.LogWarning("Timeout waiting for consumer tasks to complete");
            }
        }

        // Log any remaining in-flight entries for diagnostics
        if (!_inFlightEntries.IsEmpty)
        {
            _logger.LogWarning(
                "Consumer stopped with {Count} in-flight entries: {Entries}",
                _inFlightEntries.Count,
                string.Join(", ", _inFlightEntries.Keys.Take(10)));
        }

        _logger.LogInformation("Redis Streams consumer stopped for stream '{StreamKey}'", _streamKey);
    }

    private async Task EnsureConsumerGroupExistsAsync(CancellationToken cancellationToken)
    {
        var db = _connectionPool.GetDatabase();

        try
        {
            // Use StreamPosition.Beginning ("0") to ensure new consumer groups
            // receive all unacknowledged messages in the stream, not just new ones.
            // This is the correct behavior for a message queue where durability matters.
            await db.StreamCreateConsumerGroupAsync(
                _streamKey,
                _consumerGroup,
                StreamPosition.Beginning,
                createStream: true);

            _logger.LogDebug(
                "Created consumer group '{ConsumerGroup}' for stream '{StreamKey}'",
                _consumerGroup, _streamKey);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            _logger.LogDebug(
                "Consumer group '{ConsumerGroup}' already exists for stream '{StreamKey}'",
                _consumerGroup, _streamKey);
        }
    }

    /// <summary>
    /// Main processing loop that reads new messages from the stream.
    /// Uses XREADGROUP with position ">" to read only new, undelivered messages.
    /// </summary>
    private async Task ProcessMessagesLoopAsync(
        Func<ConsumeContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        var db = _connectionPool.GetDatabase();

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var entries = await db.StreamReadGroupAsync(
                    _streamKey,
                    _consumerGroup,
                    _consumerId,
                    position: StreamPosition.NewMessages,
                    count: _options.BatchSize,
                    noAck: false);

                if (entries.Length == 0)
                {
                    await Task.Delay(_options.BlockingTimeout, cancellationToken);
                    continue;
                }

                await ProcessEntriesBatchAsync(db, entries, handler, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (RedisException ex)
            {
                _logger.LogError(ex, "Redis error while consuming from stream '{StreamKey}'", _streamKey);
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error while consuming from stream '{StreamKey}'", _streamKey);
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
        }
    }

    /// <summary>
    /// Claiming loop that handles pending messages (retries and claims from dead consumers).
    /// Uses XREADGROUP with position "0" to re-read our pending messages and XAUTOCLAIM for others.
    /// </summary>
    private async Task ClaimIdleMessagesLoopAsync(
        Func<ConsumeContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        var db = _connectionPool.GetDatabase();

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.ClaimCheckInterval, cancellationToken);

                // Re-read our own pending messages for retry
                await RetryOwnPendingMessagesAsync(db, handler, cancellationToken);

                // Claim idle messages from other consumers
                await ClaimFromOtherConsumersAsync(db, handler, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (RedisServerException ex) when (ex.Message.Contains("NOGROUP"))
            {
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error in claiming loop for stream '{StreamKey}'", _streamKey);
            }
        }
    }

    /// <summary>
    /// Re-reads and retries our own pending messages that may have failed processing.
    /// </summary>
    private async Task RetryOwnPendingMessagesAsync(
        IDatabase db,
        Func<ConsumeContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        var pendingEntries = await db.StreamReadGroupAsync(
            _streamKey,
            _consumerGroup,
            _consumerId,
            position: "0",
            count: _options.BatchSize,
            noAck: false);

        if (pendingEntries.Length > 0)
        {
            _logger.LogDebug(
                "Found {Count} pending messages for retry in stream '{StreamKey}'",
                pendingEntries.Length, _streamKey);

            await ProcessEntriesBatchAsync(db, pendingEntries, handler, cancellationToken);
        }
    }

    /// <summary>
    /// Claims idle messages from other consumers that may have died.
    /// </summary>
    private async Task ClaimFromOtherConsumersAsync(
        IDatabase db,
        Func<ConsumeContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        var claimedMessages = await db.StreamAutoClaimAsync(
            _streamKey,
            _consumerGroup,
            _consumerId,
            (long)_options.ClaimIdleTime.TotalMilliseconds,
            "0-0",
            _options.BatchSize);

        if (claimedMessages.ClaimedEntries.Length > 0)
        {
            _logger.LogDebug(
                "Claimed {Count} idle messages from other consumers in stream '{StreamKey}'",
                claimedMessages.ClaimedEntries.Length, _streamKey);

            await ProcessEntriesBatchAsync(db, claimedMessages.ClaimedEntries, handler, cancellationToken);
        }
    }

    /// <summary>
    /// Processes a batch of entries with concurrency control and duplicate prevention.
    /// </summary>
    private async Task ProcessEntriesBatchAsync(
        IDatabase db,
        StreamEntry[] entries,
        Func<ConsumeContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        var processingTasks = new List<Task>();

        foreach (var entry in entries)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            var entryIdStr = entry.Id.ToString();

            // Atomically check if entry is already being processed
            // TryAdd returns false if the key already exists
            if (!_inFlightEntries.TryAdd(entryIdStr, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()))
            {
                _logger.LogDebug(
                    "Skipping entry {EntryId} - already in flight",
                    entryIdStr);
                continue;
            }

            await _concurrencySemaphore.WaitAsync(cancellationToken);

            var task = ProcessEntryWithTrackingAsync(db, entry, entryIdStr, handler, cancellationToken);
            processingTasks.Add(task);
        }

        // Wait for all tasks in this batch to complete to maintain backpressure
        if (processingTasks.Count > 0)
        {
            await Task.WhenAll(processingTasks);
        }
    }

    /// <summary>
    /// Processes a single entry with proper tracking and cleanup.
    /// </summary>
    private async Task ProcessEntryWithTrackingAsync(
        IDatabase db,
        StreamEntry entry,
        string entryIdStr,
        Func<ConsumeContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        try
        {
            await ProcessEntryAsync(db, entry, handler, cancellationToken);
        }
        finally
        {
            // Always remove from in-flight tracking and release semaphore
            _inFlightEntries.TryRemove(entryIdStr, out _);
            _concurrencySemaphore.Release();
        }
    }

    /// <summary>
    /// Processes a single stream entry through the handler pipeline.
    /// </summary>
    private async Task ProcessEntryAsync(
        IDatabase db,
        StreamEntry entry,
        Func<ConsumeContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        var entryId = entry.Id;
        string? messageId = null;

        try
        {
            var values = entry.Values.ToDictionary(
                v => v.Name.ToString(),
                v => v.Value);

            messageId = GetEntryValue(values, "message-id") ?? entryId.ToString();
            var messageType = GetEntryValue(values, "message-type");
            var body = GetEntryBytes(values, "body");
            var headers = ParseHeaders(GetEntryValue(values, "headers"));
            var correlationId = GetEntryValue(values, "correlation-id");

            // Get delivery count for dead letter handling
            var pendingInfo = await GetPendingInfoAsync(db, entryId);
            var deliveryCount = pendingInfo?.DeliveryCount ?? 1;

            // Check if message should be moved to DLQ
            if (deliveryCount > _options.MaxDeliveryAttempts)
            {
                _logger.LogWarning(
                    "Message {MessageId} exceeded max delivery attempts ({Count}), moving to DLQ",
                    messageId, deliveryCount);

                await MoveToDeadLetterAsync(db, entry, "Max delivery attempts exceeded");
                await AcknowledgeEntryAsync(db, entryId, messageId);
                return;
            }

            var context = BuildConsumeContext(entry, values, messageId, messageType, body, headers, correlationId, deliveryCount);

            // Execute handler with timeout
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_consumerOptions.ProcessingTimeout);

            await handler(context, cts.Token);

            // Handle acknowledgment based on context flags
            await HandleAcknowledgmentAsync(db, entry, context, messageId);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogDebug("Message processing cancelled due to shutdown: {MessageId}", messageId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing message {MessageId} from stream '{StreamKey}'",
                messageId, _streamKey);
            // Don't acknowledge - message will be retried
        }
    }

    private ConsumeContext BuildConsumeContext(
        StreamEntry entry,
        Dictionary<string, RedisValue> values,
        string messageId,
        string? messageType,
        byte[]? body,
        Dictionary<string, object> headers,
        string? correlationId,
        long deliveryCount)
    {
        var messageContext = new MessageContext(
            messageId: Guid.TryParse(messageId, out var id) ? id : Guid.NewGuid(),
            queueName: _streamKey,
            correlationId: correlationId,
            exchangeName: null,
            routingKey: _streamKey,
            headers: headers,
            deliveryCount: (int)deliveryCount);

        var context = new ConsumeContext
        {
            Body = body ?? [],
            MessageContext = messageContext,
            DeliveryTag = (ulong)entry.Id.GetHashCode(),
            Redelivered = deliveryCount > 1,
            Headers = headers,
            ContentType = GetEntryValue(values, "content-type") ?? "application/json"
        };

        if (!string.IsNullOrEmpty(messageType))
        {
            context.Data["message-type"] = messageType;
        }

        return context;
    }

    private async Task HandleAcknowledgmentAsync(
        IDatabase db,
        StreamEntry entry,
        ConsumeContext context,
        string? messageId)
    {
        if (context.ShouldAck && !context.ShouldReject)
        {
            await AcknowledgeEntryAsync(db, entry.Id, messageId);
        }
        else if (context.ShouldReject)
        {
            if (!context.RequeueOnReject)
            {
                await MoveToDeadLetterAsync(db, entry, "Message rejected by handler");
            }
            await AcknowledgeEntryAsync(db, entry.Id, messageId);
        }
    }

    private async Task AcknowledgeEntryAsync(IDatabase db, RedisValue entryId, string? messageId)
    {
        await db.StreamAcknowledgeAsync(_streamKey, _consumerGroup, entryId);
        _logger.LogDebug("Acknowledged message {MessageId}", messageId);
    }

    private async Task<StreamPendingMessageInfo?> GetPendingInfoAsync(IDatabase db, RedisValue entryId)
    {
        try
        {
            var pending = await db.StreamPendingMessagesAsync(
                _streamKey,
                _consumerGroup,
                count: 1000,
                consumerName: RedisValue.Null);

            var entryIdStr = entryId.ToString();
            return pending.FirstOrDefault(p => p.MessageId.ToString() == entryIdStr);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get pending info for entry {EntryId}", entryId);
            return null;
        }
    }

    private async Task MoveToDeadLetterAsync(IDatabase db, StreamEntry entry, string reason)
    {
        if (_options.DeadLetterStrategy == DeadLetterStrategy.Disabled)
            return;

        var dlqStreamKey = _options.DeadLetterStrategy == DeadLetterStrategy.PerConsumerGroup
            ? $"{_streamKey}:{_consumerGroup}:{_options.DeadLetterSuffix}"
            : $"{_streamKey}:{_options.DeadLetterSuffix}";

        try
        {
            var dlqEntries = entry.Values.ToList();
            dlqEntries.Add(new NameValueEntry("dlq-reason", reason));
            dlqEntries.Add(new NameValueEntry("dlq-timestamp", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString()));
            dlqEntries.Add(new NameValueEntry("dlq-source-stream", _streamKey));
            dlqEntries.Add(new NameValueEntry("dlq-source-id", entry.Id.ToString()));

            await db.StreamAddAsync(dlqStreamKey, [.. dlqEntries]);

            _logger.LogDebug(
                "Moved message {EntryId} to dead letter stream '{DlqStreamKey}'",
                entry.Id, dlqStreamKey);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to move message {EntryId} to dead letter queue", entry.Id);
        }
    }

    private static string? GetEntryValue(Dictionary<string, RedisValue> values, string key)
    {
        if (values.TryGetValue(key, out var value) && !value.IsNullOrEmpty)
        {
            return value.ToString();
        }
        return null;
    }

    private static byte[]? GetEntryBytes(Dictionary<string, RedisValue> values, string key)
    {
        if (values.TryGetValue(key, out var value) && !value.IsNullOrEmpty)
        {
            return (byte[]?)value;
        }
        return null;
    }

    private static Dictionary<string, object> ParseHeaders(string? headersJson)
    {
        if (string.IsNullOrEmpty(headersJson))
            return [];

        try
        {
            var parsed = JsonSerializer.Deserialize<Dictionary<string, string>>(headersJson);
            return parsed?.ToDictionary(kvp => kvp.Key, kvp => (object)kvp.Value) ?? [];
        }
        catch
        {
            return [];
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _concurrencySemaphore.Dispose();
        _stoppingCts.Dispose();
    }
}
