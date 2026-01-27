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

    /// <summary>
    /// Cache of pending message info fetched at batch level.
    /// Key: Redis entry ID string, Value: delivery count.
    /// This avoids expensive per-message XPENDING calls.
    /// </summary>
    private readonly ConcurrentDictionary<string, int> _deliveryCountCache = new();

    /// <summary>
    /// Tracks recently acknowledged entry IDs to prevent duplicate processing.
    /// Key: Redis entry ID string, Value: timestamp when acknowledged.
    /// This prevents race conditions where the claiming loop receives stale pending entries
    /// that were acked between the pending list fetch and processing attempt.
    /// </summary>
    private readonly ConcurrentDictionary<string, long> _recentlyAcked = new();

    /// <summary>
    /// How long to keep entries in the recently acked cache.
    /// Should be longer than the claiming check interval to ensure stale entries are caught.
    /// </summary>
    private static readonly TimeSpan RecentlyAckedRetention = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Tracks active processing tasks for graceful shutdown.
    /// Using ConcurrentBag for thread-safe add and enumeration during shutdown.
    /// </summary>
    private readonly ConcurrentBag<Task> _activeTasks = new();

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

        await EnsureConsumerGroupExistsAsync(cancellationToken).ConfigureAwait(false);

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
        await _stoppingCts.CancelAsync().ConfigureAwait(false);

        // Wait for processing loop tasks to complete
        var loopTasks = new List<Task>();
        if (_processingTask != null) loopTasks.Add(_processingTask);
        if (_claimingTask != null) loopTasks.Add(_claimingTask);

        if (loopTasks.Count > 0)
        {
            try
            {
                await Task.WhenAll(loopTasks).WaitAsync(TimeSpan.FromSeconds(10), cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
            catch (TimeoutException)
            {
                _logger.LogWarning("Timeout waiting for consumer loop tasks to complete");
            }
        }

        // Wait for active message processing tasks to complete (graceful drain)
        var activeTasks = _activeTasks.Where(t => !t.IsCompleted).ToArray();
        if (activeTasks.Length > 0)
        {
            _logger.LogDebug("Waiting for {Count} active processing tasks to complete", activeTasks.Length);
            try
            {
                await Task.WhenAll(activeTasks).WaitAsync(TimeSpan.FromSeconds(30), cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
            catch (TimeoutException)
            {
                _logger.LogWarning("Timeout waiting for {Count} active processing tasks", activeTasks.Length);
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
                createStream: true).ConfigureAwait(false);

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
                    noAck: false).ConfigureAwait(false);

                if (entries.Length == 0)
                {
                    await Task.Delay(_options.BlockingTimeout, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // New messages have delivery count of 1
                await ProcessEntriesBatchAsync(db, entries, handler, cancellationToken, isFromPendingRead: false).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (RedisException ex)
            {
                _logger.LogError(ex, "Redis error while consuming from stream '{StreamKey}'", _streamKey);
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error while consuming from stream '{StreamKey}'", _streamKey);
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
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
                await Task.Delay(_options.ClaimCheckInterval, cancellationToken).ConfigureAwait(false);

                // Clean up old entries from the recently acked cache
                CleanupRecentlyAckedCache();

                // Re-read our own pending messages for retry
                await RetryOwnPendingMessagesAsync(db, handler, cancellationToken).ConfigureAwait(false);

                // Claim idle messages from other consumers
                await ClaimFromOtherConsumersAsync(db, handler, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (RedisServerException ex) when (ex.Message.Contains("NOGROUP"))
            {
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
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
            noAck: false).ConfigureAwait(false);

        if (pendingEntries.Length > 0)
        {
            _logger.LogDebug(
                "Found {Count} pending messages for retry in stream '{StreamKey}'",
                pendingEntries.Length, _streamKey);

            // Pending messages need delivery count tracking for dead letter handling
            await ProcessEntriesBatchAsync(db, pendingEntries, handler, cancellationToken, isFromPendingRead: true).ConfigureAwait(false);
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
            _options.BatchSize).ConfigureAwait(false);

        if (claimedMessages.ClaimedEntries.Length > 0)
        {
            _logger.LogDebug(
                "Claimed {Count} idle messages from other consumers in stream '{StreamKey}'",
                claimedMessages.ClaimedEntries.Length, _streamKey);

            // Claimed messages are pending messages that need delivery count tracking
            await ProcessEntriesBatchAsync(db, claimedMessages.ClaimedEntries, handler, cancellationToken, isFromPendingRead: true).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Processes a batch of entries with concurrency control and duplicate prevention.
    /// Pre-fetches delivery counts for the batch to avoid per-message Redis calls.
    /// Uses continuous flow - tasks are started immediately and the semaphore provides backpressure.
    /// </summary>
    private async Task ProcessEntriesBatchAsync(
        IDatabase db,
        StreamEntry[] entries,
        Func<ConsumeContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken,
        bool isFromPendingRead = false)
    {
        // Pre-fetch delivery counts for the entire batch if processing pending messages
        // This avoids O(n) Redis calls per message
        if (isFromPendingRead && entries.Length > 0)
        {
            await PreFetchDeliveryCountsAsync(db, entries, cancellationToken).ConfigureAwait(false);
        }

        foreach (var entry in entries)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            var entryIdStr = entry.Id.ToString();

            // For pending reads, also check if entry was recently acked.
            // This prevents race conditions where the claiming loop receives stale pending entries
            // that were acked between the pending list fetch and this processing attempt.
            if (isFromPendingRead && _recentlyAcked.ContainsKey(entryIdStr))
            {
                _logger.LogDebug(
                    "Skipping entry {EntryId} - recently acknowledged",
                    entryIdStr);
                continue;
            }

            // Atomically check if entry is already being processed
            // TryAdd returns false if the key already exists
            if (!_inFlightEntries.TryAdd(entryIdStr, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()))
            {
                _logger.LogDebug(
                    "Skipping entry {EntryId} - already in flight",
                    entryIdStr);
                continue;
            }

            // Wait for semaphore - this provides backpressure when MaxConcurrency is reached.
            // This is the key to continuous flow: we wait here only when at capacity,
            // not for the entire batch to complete.
            await _concurrencySemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            // Start processing without awaiting - the semaphore controls concurrency.
            // Task exceptions are handled in ProcessEntryWithTrackingAsync.
            var task = ProcessEntryWithTrackingAsync(db, entry, entryIdStr, handler, cancellationToken, isFromPendingRead);

            // Track task for graceful shutdown
            _activeTasks.Add(task);
        }

        // Note: We don't wait for tasks here - continuous flow allows the main loop
        // to immediately read the next batch. The semaphore provides backpressure.
    }

    /// <summary>
    /// Pre-fetches delivery counts for a batch of entries in a single Redis call.
    /// </summary>
    private async Task PreFetchDeliveryCountsAsync(
        IDatabase db,
        StreamEntry[] entries,
        CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return;

        try
        {
            // Fetch pending info for all messages owned by this consumer
            var pending = await db.StreamPendingMessagesAsync(
                _streamKey,
                _consumerGroup,
                count: Math.Max(entries.Length * 2, 100),
                consumerName: _consumerId).ConfigureAwait(false);

            // Build lookup dictionary
            foreach (var info in pending)
            {
                var entryIdStr = info.MessageId.ToString();
                _deliveryCountCache[entryIdStr] = (int)info.DeliveryCount;
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to pre-fetch delivery counts, will use defaults");
        }
    }

    /// <summary>
    /// Processes a single entry with proper tracking and cleanup.
    /// Handles exceptions to prevent unobserved task exceptions since tasks are not immediately awaited.
    /// </summary>
    private async Task ProcessEntryWithTrackingAsync(
        IDatabase db,
        StreamEntry entry,
        string entryIdStr,
        Func<ConsumeContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken,
        bool isFromPendingRead = false)
    {
        try
        {
            await ProcessEntryAsync(db, entry, entryIdStr, handler, cancellationToken, isFromPendingRead).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Expected during shutdown, don't log
        }
        catch (Exception ex)
        {
            // Log unexpected exceptions to prevent them from being unobserved.
            // The message will be retried via the pending mechanism.
            _logger.LogError(ex, "Unexpected error processing entry {EntryId}", entryIdStr);
        }
        finally
        {
            // Always remove from in-flight tracking, delivery count cache, and release semaphore
            _inFlightEntries.TryRemove(entryIdStr, out _);
            _deliveryCountCache.TryRemove(entryIdStr, out _);
            _concurrencySemaphore.Release();
        }
    }

    /// <summary>
    /// Processes a single stream entry through the handler pipeline.
    /// </summary>
    private async Task ProcessEntryAsync(
        IDatabase db,
        StreamEntry entry,
        string entryIdStr,
        Func<ConsumeContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken,
        bool isFromPendingRead = false)
    {
        var entryId = entry.Id;
        string? messageId = null;

        try
        {
            var values = entry.Values;

            messageId = GetEntryValue(values, "message-id") ?? entryIdStr;
            var messageType = GetEntryValue(values, "message-type");
            var body = GetEntryBytes(values, "body");
            var headers = ParseHeaders(GetEntryValue(values, "headers"));
            var correlationId = GetEntryValue(values, "correlation-id");

            // Get delivery count from cache (populated at batch level) or use default
            // For new messages (not from pending), delivery count is always 1
            var deliveryCount = 1;
            if (isFromPendingRead && _deliveryCountCache.TryGetValue(entryIdStr, out var cachedCount))
            {
                deliveryCount = cachedCount;
            }

            // Check if message should be moved to DLQ
            if (deliveryCount > _options.MaxDeliveryAttempts)
            {
                _logger.LogWarning(
                    "Message {MessageId} exceeded max delivery attempts ({Count}), moving to DLQ",
                    messageId, deliveryCount);

                await MoveToDeadLetterAsync(db, entry, "Max delivery attempts exceeded").ConfigureAwait(false);
                await AcknowledgeEntryAsync(db, entryId, messageId).ConfigureAwait(false);
                return;
            }

            var context = BuildConsumeContext(entry, values, messageId, messageType, body, headers, correlationId, deliveryCount);

            // Execute handler with timeout
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_consumerOptions.ProcessingTimeout);

            await handler(context, cts.Token).ConfigureAwait(false);

            // Handle acknowledgment based on context flags
            await HandleAcknowledgmentAsync(db, entry, context, messageId).ConfigureAwait(false);
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
        NameValueEntry[] values,
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
            await AcknowledgeEntryAsync(db, entry.Id, messageId).ConfigureAwait(false);
        }
        else if (context.ShouldReject)
        {
            if (!context.RequeueOnReject)
            {
                await MoveToDeadLetterAsync(db, entry, "Message rejected by handler").ConfigureAwait(false);
            }
            await AcknowledgeEntryAsync(db, entry.Id, messageId).ConfigureAwait(false);
        }
    }

    private async Task AcknowledgeEntryAsync(IDatabase db, RedisValue entryId, string? messageId)
    {
        await db.StreamAcknowledgeAsync(_streamKey, _consumerGroup, entryId).ConfigureAwait(false);

        // Track this entry as recently acked to prevent race conditions with the claiming loop.
        // The claiming loop might receive stale pending list data that includes entries
        // that were acked between the fetch and the processing attempt.
        var entryIdStr = entryId.ToString();
        _recentlyAcked[entryIdStr] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        _logger.LogDebug("Acknowledged message {MessageId}", messageId);
    }

    /// <summary>
    /// Removes old entries from the recently acked cache to prevent unbounded growth.
    /// </summary>
    private void CleanupRecentlyAckedCache()
    {
        var cutoff = DateTimeOffset.UtcNow.Add(-RecentlyAckedRetention).ToUnixTimeMilliseconds();
        var keysToRemove = _recentlyAcked
            .Where(kvp => kvp.Value < cutoff)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (var key in keysToRemove)
        {
            _recentlyAcked.TryRemove(key, out _);
        }

        if (keysToRemove.Count > 0)
        {
            _logger.LogDebug("Cleaned up {Count} entries from recently acked cache", keysToRemove.Count);
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

            await db.StreamAddAsync(dlqStreamKey, [.. dlqEntries]).ConfigureAwait(false);

            _logger.LogDebug(
                "Moved message {EntryId} to dead letter stream '{DlqStreamKey}'",
                entry.Id, dlqStreamKey);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to move message {EntryId} to dead letter queue", entry.Id);
        }
    }

    private static string? GetEntryValue(NameValueEntry[] values, string key)
    {
        foreach (var entry in values)
        {
            if (entry.Name == key && !entry.Value.IsNullOrEmpty)
                return entry.Value.ToString();
        }
        return null;
    }

    private static byte[]? GetEntryBytes(NameValueEntry[] values, string key)
    {
        foreach (var entry in values)
        {
            if (entry.Name == key && !entry.Value.IsNullOrEmpty)
                return (byte[]?)entry.Value;
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
        await StopAsync().ConfigureAwait(false);
        _concurrencySemaphore.Dispose();
        _stoppingCts.Dispose();
    }
}
