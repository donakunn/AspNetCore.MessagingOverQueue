using Donakunn.MessagingOverQueue.RedisStreams.Configuration;
using Donakunn.MessagingOverQueue.RedisStreams.Connection;
using Donakunn.MessagingOverQueue.Topology.Abstractions;
using Donakunn.MessagingOverQueue.Topology.Attributes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Reflection;

namespace Donakunn.MessagingOverQueue.RedisStreams;

/// <summary>
/// Redis Streams implementation of the topology declarer.
/// Creates streams and consumer groups based on topology definitions.
/// </summary>
public sealed class RedisStreamsTopologyDeclarer : ITopologyDeclarer
{
    private readonly IRedisConnectionPool _connectionPool;
    private readonly ITopologyRegistry _registry;
    private readonly RedisStreamsOptions _options;
    private readonly ILogger<RedisStreamsTopologyDeclarer> _logger;
    private readonly HashSet<string> _declaredStreams = [];
    private readonly HashSet<string> _declaredGroups = [];
    private readonly Lock _lock = new();

    public RedisStreamsTopologyDeclarer(
        IRedisConnectionPool connectionPool,
        ITopologyRegistry registry,
        IOptions<RedisStreamsOptions> options,
        ILogger<RedisStreamsTopologyDeclarer> logger)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task DeclareAsync(TopologyDefinition definition, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);

        var db = _connectionPool.GetDatabase();
        var streamKey = BuildStreamKey(definition);
        var consumerGroup = GetConsumerGroupName(definition);

        // Declare the main stream and consumer group
        await DeclareStreamAndGroupAsync(db, streamKey, consumerGroup, cancellationToken);

        // Declare DLQ if configured
        if (definition.DeadLetter != null && _options.DeadLetterStrategy != DeadLetterStrategy.Disabled)
        {
            var dlqStreamKey = BuildDeadLetterStreamKey(streamKey, consumerGroup);
            await EnsureStreamExistsAsync(db, dlqStreamKey, cancellationToken);
        }

        _logger.LogDebug(
            "Declared topology for {MessageType}: Stream={StreamKey}, ConsumerGroup={ConsumerGroup}",
            definition.MessageType.Name, streamKey, consumerGroup);
    }

    /// <inheritdoc />
    public async Task DeclareAllAsync(IEnumerable<TopologyDefinition> definitions, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definitions);

        foreach (var definition in definitions)
        {
            await DeclareAsync(definition, cancellationToken);
        }
    }

    /// <inheritdoc />
    public async Task DeclareRegisteredTopologiesAsync(CancellationToken cancellationToken = default)
    {
        var definitions = _registry.GetAllTopologies();

        if (definitions.Count == 0)
        {
            _logger.LogDebug("No topologies registered to declare");
            return;
        }

        _logger.LogInformation("Declaring {Count} registered topologies", definitions.Count);
        await DeclareAllAsync(definitions, cancellationToken);
    }

    private async Task DeclareStreamAndGroupAsync(
        IDatabase db,
        string streamKey,
        string consumerGroup,
        CancellationToken cancellationToken)
    {
        var groupKey = $"{streamKey}:{consumerGroup}";

        lock (_lock)
        {
            if (_declaredGroups.Contains(groupKey))
                return;
        }

        try
        {
            // XGROUP CREATE with MKSTREAM creates both stream and group atomically
            await db.StreamCreateConsumerGroupAsync(
                streamKey,
                consumerGroup,
                StreamPosition.NewMessages,
                createStream: true);

            lock (_lock)
            {
                _declaredStreams.Add(streamKey);
                _declaredGroups.Add(groupKey);
            }

            _logger.LogDebug(
                "Created consumer group '{ConsumerGroup}' for stream '{StreamKey}'",
                consumerGroup, streamKey);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            // Consumer group already exists - this is expected and fine
            lock (_lock)
            {
                _declaredStreams.Add(streamKey);
                _declaredGroups.Add(groupKey);
            }

            _logger.LogDebug(
                "Consumer group '{ConsumerGroup}' already exists for stream '{StreamKey}'",
                consumerGroup, streamKey);
        }
    }

    private async Task EnsureStreamExistsAsync(
        IDatabase db,
        string streamKey,
        CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (_declaredStreams.Contains(streamKey))
                return;
        }

        try
        {
            // Check if stream exists
            var exists = await db.KeyExistsAsync(streamKey);

            if (!exists)
            {
                // Create an empty stream by adding and immediately deleting a message
                // Or use XGROUP CREATE MKSTREAM with a dummy group
                await db.StreamCreateConsumerGroupAsync(
                    streamKey,
                    "__init__",
                    StreamPosition.NewMessages,
                    createStream: true);

                // Remove the dummy group
                await db.StreamDeleteConsumerGroupAsync(streamKey, "__init__");
            }

            lock (_lock)
            {
                _declaredStreams.Add(streamKey);
            }

            _logger.LogDebug("Ensured stream '{StreamKey}' exists", streamKey);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            // Stream/group already exists
            lock (_lock)
            {
                _declaredStreams.Add(streamKey);
            }
        }
    }

    /// <summary>
    /// Builds the Redis stream key from the topology definition.
    /// Uses queue name to match publisher and consumer stream key format.
    /// </summary>
    private string BuildStreamKey(TopologyDefinition definition)
    {
        // Use queue name as stream key (matches publisher and consumer)
        var streamName = definition.Queue.Name;

        if (string.IsNullOrEmpty(_options.StreamPrefix))
        {
            return streamName;
        }

        return $"{_options.StreamPrefix}:{streamName}";
    }

    /// <summary>
    /// Gets the consumer group name for a topology definition.
    /// Checks for RedisConsumerGroupAttribute override, otherwise uses queue name.
    /// </summary>
    private static string GetConsumerGroupName(TopologyDefinition definition)
    {
        // Check for attribute override on message type
        var attribute = definition.MessageType.GetCustomAttribute<RedisConsumerGroupAttribute>();
        if (attribute != null)
        {
            return attribute.GroupName;
        }

        // Default to queue name from topology
        return definition.Queue.Name;
    }

    /// <summary>
    /// Builds the dead letter stream key based on strategy.
    /// </summary>
    private string BuildDeadLetterStreamKey(string streamKey, string consumerGroup)
    {
        return _options.DeadLetterStrategy == DeadLetterStrategy.PerConsumerGroup
            ? $"{streamKey}:{consumerGroup}:{_options.DeadLetterSuffix}"
            : $"{streamKey}:{_options.DeadLetterSuffix}";
    }
}
