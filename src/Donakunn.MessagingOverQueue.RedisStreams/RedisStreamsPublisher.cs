using Donakunn.MessagingOverQueue.Providers;
using Donakunn.MessagingOverQueue.Publishing.Middleware;
using Donakunn.MessagingOverQueue.RedisStreams.Configuration;
using Donakunn.MessagingOverQueue.RedisStreams.Connection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Text.Json;

namespace Donakunn.MessagingOverQueue.RedisStreams;

/// <summary>
/// Redis Streams implementation of the internal publisher.
/// Publishes messages to Redis Streams with automatic trimming and retention.
/// </summary>
public sealed class RedisStreamsPublisher : IInternalPublisher
{
    private readonly IRedisConnectionPool _connectionPool;
    private readonly RedisStreamsOptions _options;
    private readonly ILogger<RedisStreamsPublisher> _logger;

    public RedisStreamsPublisher(
        IRedisConnectionPool connectionPool,
        IOptions<RedisStreamsOptions> options,
        ILogger<RedisStreamsPublisher> logger)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task PublishAsync(PublishContext context, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        var streamKey = BuildStreamKey(context);
        var db = _connectionPool.GetDatabase();

        // Extract message ID - store our GUID in the payload (Redis generates stream entry ID)
        var messageId = context.Message?.Id.ToString()
            ?? GetHeaderValue(context.Headers, "message-id")
            ?? Guid.NewGuid().ToString();

        var correlationId = context.Message?.CorrelationId
            ?? GetHeaderValue(context.Headers, "correlation-id");

        var causationId = context.Message?.CausationId
            ?? GetHeaderValue(context.Headers, "causation-id");

        var messageType = context.MessageType?.AssemblyQualifiedName
            ?? context.MessageType?.FullName
            ?? GetHeaderValue(context.Headers, "message-type")
            ?? "unknown";

        // Build stream entry values
        var entries = new NameValueEntry[]
        {
            new("message-id", messageId),
            new("message-type", messageType),
            new("body", context.Body ?? Array.Empty<byte>()),
            new("headers", SerializeHeaders(context.Headers)),
            new("timestamp", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString()),
            new("correlation-id", correlationId ?? string.Empty),
            new("causation-id", causationId ?? string.Empty),
            new("content-type", context.ContentType ?? "application/json"),
            new("persistent", context.Persistent.ToString())
        };

        try
        {
            // Determine trimming strategy
            var redisStreamId = await AddToStreamAsync(db, streamKey, entries, cancellationToken);

            _logger.LogDebug(
                "Published message {MessageId} to stream '{StreamKey}' with entry ID {EntryId}",
                messageId, streamKey, redisStreamId);

            // Apply time-based trimming if configured (done asynchronously for performance)
            if (_options.RetentionStrategy == StreamRetentionStrategy.TimeBased)
            {
                _ = TrimByTimeAsync(db, streamKey);
            }
        }
        catch (RedisException ex)
        {
            _logger.LogError(ex, "Failed to publish message {MessageId} to stream '{StreamKey}'",
                messageId, streamKey);
            throw;
        }
    }

    private async Task<RedisValue> AddToStreamAsync(
        IDatabase db,
        string streamKey,
        NameValueEntry[] entries,
        CancellationToken cancellationToken)
    {
        // Apply count-based retention during add if configured
        int? maxLength = _options.RetentionStrategy == StreamRetentionStrategy.CountBased
            ? (int?)_options.MaxStreamLength
            : null;

        var redisStreamId = await db.StreamAddAsync(
            streamKey,
            entries,
            maxLength: maxLength,
            useApproximateMaxLength: _options.ApproximateTrimming);

        return redisStreamId;
    }

    private async Task TrimByTimeAsync(IDatabase db, string streamKey)
    {
        try
        {
            // Calculate minimum ID based on retention period
            var minTimestamp = DateTimeOffset.UtcNow.Subtract(_options.RetentionPeriod).ToUnixTimeMilliseconds();
            var minId = $"{minTimestamp}-0";

            // Use XTRIM with MINID strategy via Lua script for Redis 6.2+
            var script = @"
                local streamKey = KEYS[1]
                local minId = ARGV[1]
                return redis.call('XTRIM', streamKey, 'MINID', '~', minId)
            ";

            await db.ScriptEvaluateAsync(script, new RedisKey[] { streamKey }, new RedisValue[] { minId });

            _logger.LogDebug(
                "Trimmed stream '{StreamKey}' to messages after {MinId}",
                streamKey, minId);
        }
        catch (Exception ex)
        {
            // Don't fail the publish operation for trimming errors
            _logger.LogWarning(ex, "Failed to trim stream '{StreamKey}'", streamKey);
        }
    }

    /// <summary>
    /// Builds the Redis stream key from the publish context.
    /// Format: {prefix}:{queueName}
    /// Uses queue name to match consumer stream key format.
    /// </summary>
    private string BuildStreamKey(PublishContext context)
    {
        // Prefer queue name for stream key (matches consumer's stream key format)
        var streamName = context.QueueName;

        // Fallback to routing key if queue name not available
        if (string.IsNullOrEmpty(streamName))
        {
            streamName = context.RoutingKey;
        }

        // Last resort: use exchange name
        if (string.IsNullOrEmpty(streamName))
        {
            streamName = context.ExchangeName;
        }

        if (string.IsNullOrEmpty(streamName))
        {
            throw new InvalidOperationException(
                "Cannot determine stream key: QueueName, RoutingKey, and ExchangeName are all empty.");
        }

        if (string.IsNullOrEmpty(_options.StreamPrefix))
        {
            return streamName;
        }

        return $"{_options.StreamPrefix}:{streamName}";
    }

    private static string? GetHeaderValue(Dictionary<string, object?> headers, string key)
    {
        if (headers.TryGetValue(key, out var value))
        {
            return value?.ToString();
        }
        return null;
    }

    private static string SerializeHeaders(Dictionary<string, object?> headers)
    {
        if (headers.Count == 0)
            return "{}";

        // Convert to string dictionary for serialization
        var stringHeaders = headers
            .Where(h => h.Value != null)
            .ToDictionary(h => h.Key, h => h.Value?.ToString() ?? string.Empty);

        return JsonSerializer.Serialize(stringHeaders);
    }
}
