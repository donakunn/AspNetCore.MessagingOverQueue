using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Providers;
using Donakunn.MessagingOverQueue.RedisStreams.Configuration;
using Donakunn.MessagingOverQueue.RedisStreams.Connection;
using Donakunn.MessagingOverQueue.RedisStreams.HealthChecks;
using Donakunn.MessagingOverQueue.Topology.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Donakunn.MessagingOverQueue.RedisStreams;

/// <summary>
/// Redis Streams implementation of the messaging provider.
/// Provides high-performance message streaming using Redis Streams.
/// </summary>
public sealed class RedisStreamsMessagingProvider : IMessagingProvider
{
    private readonly IRedisConnectionPool _connectionPool;
    private readonly ITopologyDeclarer _topologyDeclarer;
    private readonly IServiceProvider _serviceProvider;
    private readonly RedisStreamsOptions _options;
    private readonly ILogger<RedisStreamsMessagingProvider> _logger;
    private bool _disposed;

    public RedisStreamsMessagingProvider(
        IRedisConnectionPool connectionPool,
        ITopologyDeclarer topologyDeclarer,
        IServiceProvider serviceProvider,
        IOptions<RedisStreamsOptions> options,
        ILogger<RedisStreamsMessagingProvider> logger)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
        _topologyDeclarer = topologyDeclarer ?? throw new ArgumentNullException(nameof(topologyDeclarer));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public string ProviderName => "RedisStreams";

    /// <inheritdoc />
    public bool IsConnected => _connectionPool.IsConnected;

    /// <inheritdoc />
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Initializing Redis Streams messaging provider");
        await _connectionPool.EnsureConnectedAsync(cancellationToken);
        _logger.LogInformation("Redis Streams messaging provider initialized");
    }

    /// <inheritdoc />
    public Task<IInternalPublisher> CreatePublisherAsync(CancellationToken cancellationToken = default)
    {
        var publisher = _serviceProvider.GetRequiredService<RedisStreamsPublisher>();
        return Task.FromResult<IInternalPublisher>(publisher);
    }

    /// <inheritdoc />
    public Task<IInternalConsumer> CreateConsumerAsync(ConsumerOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);

        var consumerLogger = _serviceProvider.GetRequiredService<ILogger<RedisStreamsConsumer>>();
        var redisOptions = _serviceProvider.GetRequiredService<IOptions<RedisStreamsOptions>>();

        // Build stream key from queue name (which follows topology conventions)
        var streamKey = BuildStreamKeyFromQueueName(options.QueueName);
        var consumerGroup = options.QueueName; // Use queue name as consumer group by default

        var consumer = new RedisStreamsConsumer(
            _connectionPool,
            redisOptions,
            options,
            streamKey,
            consumerGroup,
            consumerLogger);

        return Task.FromResult<IInternalConsumer>(consumer);
    }

    /// <inheritdoc />
    public async Task DeclareTopologyAsync(TopologyDefinition definition, CancellationToken cancellationToken = default)
    {
        await _topologyDeclarer.DeclareAsync(definition, cancellationToken);
    }

    /// <inheritdoc />
    public IHealthCheck CreateHealthCheck()
    {
        return _serviceProvider.GetRequiredService<RedisStreamsHealthCheck>();
    }

    /// <summary>
    /// Builds a stream key from a queue name.
    /// Queue names follow the pattern: service-name.message-type
    /// Stream keys follow the pattern: prefix:service-name.message-type
    /// </summary>
    private string BuildStreamKeyFromQueueName(string queueName)
    {
        if (string.IsNullOrEmpty(_options.StreamPrefix))
        {
            return queueName;
        }

        return $"{_options.StreamPrefix}:{queueName}";
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await _connectionPool.DisposeAsync();
        _logger.LogInformation("Redis Streams messaging provider disposed");
    }
}
