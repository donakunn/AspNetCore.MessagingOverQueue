using Donakunn.MessagingOverQueue.Abstractions.Consuming;
using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.Providers;
using Donakunn.MessagingOverQueue.RedisStreams.Configuration;
using Donakunn.MessagingOverQueue.RedisStreams.Connection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Text.Json;

namespace Donakunn.MessagingOverQueue.RedisStreams;

/// <summary>
/// Redis Streams implementation of the internal consumer.
/// Consumes messages using consumer groups with automatic claiming of idle messages.
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
        _processingTask = ProcessMessagesAsync(handler, _stoppingCts.Token);

        // Start the idle message claiming loop
        _claimingTask = ClaimIdleMessagesAsync(handler, _stoppingCts.Token);

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

        _logger.LogInformation("Redis Streams consumer stopped for stream '{StreamKey}'", _streamKey);
    }

    private async Task EnsureConsumerGroupExistsAsync(CancellationToken cancellationToken)
    {
        var db = _connectionPool.GetDatabase();

        try
        {
            // Try to create the consumer group (XGROUP CREATE with MKSTREAM)
            await db.StreamCreateConsumerGroupAsync(
                _streamKey,
                _consumerGroup,
                StreamPosition.NewMessages,
                createStream: true);

            _logger.LogDebug(
                "Created consumer group '{ConsumerGroup}' for stream '{StreamKey}'",
                _consumerGroup, _streamKey);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            // Consumer group already exists - this is expected
            _logger.LogDebug(
                "Consumer group '{ConsumerGroup}' already exists for stream '{StreamKey}'",
                _consumerGroup, _streamKey);
        }
    }

    private async Task ProcessMessagesAsync(
        Func<ConsumeContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        var db = _connectionPool.GetDatabase();

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Read new messages from the stream
                var entries = await db.StreamReadGroupAsync(
                    _streamKey,
                    _consumerGroup,
                    _consumerId,
                    position: StreamPosition.NewMessages,
                    count: _options.BatchSize,
                    noAck: false);

                if (entries.Length == 0)
                {
                    // No messages available, wait before polling again
                    await Task.Delay(_options.BlockingTimeout, cancellationToken);
                    continue;
                }

                // Process each message
                foreach (var entry in entries)
                {
                    if (cancellationToken.IsCancellationRequested)
                        break;

                    await _concurrencySemaphore.WaitAsync(cancellationToken);

                    // Process in background to allow concurrent handling
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await ProcessEntryAsync(db, entry, handler, cancellationToken);
                        }
                        finally
                        {
                            _concurrencySemaphore.Release();
                        }
                    }, cancellationToken);
                }
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

    private async Task ClaimIdleMessagesAsync(
        Func<ConsumeContext, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        var db = _connectionPool.GetDatabase();

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.ClaimCheckInterval, cancellationToken);

                // Use XAUTOCLAIM to claim idle messages (Redis 6.2+)
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
                        "Claimed {Count} idle messages from stream '{StreamKey}'",
                        claimedMessages.ClaimedEntries.Length, _streamKey);

                    foreach (var entry in claimedMessages.ClaimedEntries)
                    {
                        if (cancellationToken.IsCancellationRequested)
                            break;

                        await _concurrencySemaphore.WaitAsync(cancellationToken);

                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                await ProcessEntryAsync(db, entry, handler, cancellationToken);
                            }
                            finally
                            {
                                _concurrencySemaphore.Release();
                            }
                        }, cancellationToken);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (RedisServerException ex) when (ex.Message.Contains("NOGROUP"))
            {
                // Consumer group doesn't exist yet, will be created by main loop
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error claiming idle messages from stream '{StreamKey}'", _streamKey);
            }
        }
    }

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
            // Parse the stream entry
            var values = entry.Values.ToDictionary(
                v => v.Name.ToString(),
                v => v.Value);

            messageId = GetEntryValue(values, "message-id") ?? entryId.ToString();
            var messageType = GetEntryValue(values, "message-type");
            var body = GetEntryBytes(values, "body");
            var headers = ParseHeaders(GetEntryValue(values, "headers"));
            var correlationId = GetEntryValue(values, "correlation-id");
            var timestamp = GetEntryValue(values, "timestamp");

            // Check delivery count for dead letter handling
            var pendingInfo = await GetPendingInfoAsync(db, entryId);
            var deliveryCount = pendingInfo?.DeliveryCount ?? 1;

            if (deliveryCount > _options.MaxDeliveryAttempts)
            {
                _logger.LogWarning(
                    "Message {MessageId} exceeded max delivery attempts ({Count}), moving to DLQ",
                    messageId, deliveryCount);

                await MoveToDeadLetterAsync(db, entry, "Max delivery attempts exceeded");
                await db.StreamAcknowledgeAsync(_streamKey, _consumerGroup, entryId);
                return;
            }

            // Build consume context
            var messageContext = new MessageContext(
                messageId: Guid.TryParse(messageId, out var id) ? id : Guid.NewGuid(),
                queueName: _streamKey,
                correlationId: correlationId,
                exchangeName: null,
                routingKey: _streamKey,
                headers: headers!,
                deliveryCount: (int)deliveryCount);

            var context = new ConsumeContext
            {
                Body = body ?? [],
                MessageContext = messageContext,
                DeliveryTag = (ulong)entryId.GetHashCode(),
                Redelivered = deliveryCount > 1,
                Headers = headers,
                ContentType = GetEntryValue(values, "content-type") ?? "application/json"
            };

            // Add message type to headers for deserialization
            if (!string.IsNullOrEmpty(messageType))
            {
                context.Data["message-type"] = messageType;
            }

            // Invoke the handler pipeline
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_consumerOptions.ProcessingTimeout);

            await handler(context, cts.Token);

            // Acknowledge on success
            if (context.ShouldAck && !context.ShouldReject)
            {
                await db.StreamAcknowledgeAsync(_streamKey, _consumerGroup, entryId);
                _logger.LogDebug("Acknowledged message {MessageId}", messageId);
            }
            else if (context.ShouldReject)
            {
                if (!context.RequeueOnReject)
                {
                    // Move to DLQ
                    await MoveToDeadLetterAsync(db, entry, "Message rejected by handler");
                }
                await db.StreamAcknowledgeAsync(_streamKey, _consumerGroup, entryId);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogDebug("Message processing cancelled due to shutdown: {MessageId}", messageId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing message {MessageId} from stream '{StreamKey}'",
                messageId, _streamKey);

            // Don't acknowledge - message will be reclaimed after idle timeout
        }
    }

    private async Task<StreamPendingMessageInfo?> GetPendingInfoAsync(IDatabase db, RedisValue entryId)
    {
        try
        {
            var pending = await db.StreamPendingMessagesAsync(
                _streamKey,
                _consumerGroup,
                count: 1,
                consumerName: _consumerId,
                minId: entryId,
                maxId: entryId);

            return pending.FirstOrDefault();
        }
        catch
        {
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
            // Add original entry values plus error info
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
