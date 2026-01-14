using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Connection;
using Donakunn.MessagingOverQueue.Consuming;
using Donakunn.MessagingOverQueue.Consuming.Handlers;
using Donakunn.MessagingOverQueue.HealthChecks;
using Donakunn.MessagingOverQueue.Publishing;
using Donakunn.MessagingOverQueue.Publishing.Middleware;
using Donakunn.MessagingOverQueue.Topology;
using Donakunn.MessagingOverQueue.Topology.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace Donakunn.MessagingOverQueue.Providers.RabbitMq;

/// <summary>
/// RabbitMQ implementation of the messaging provider.
/// Wraps existing RabbitMQ infrastructure components.
/// </summary>
public sealed class RabbitMqMessagingProvider : IMessagingProvider
{
    private readonly IRabbitMqConnectionPool _connectionPool;
    private readonly ITopologyDeclarer _topologyDeclarer;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<RabbitMqMessagingProvider> _logger;
    private bool _disposed;

    public RabbitMqMessagingProvider(
        IRabbitMqConnectionPool connectionPool,
        ITopologyDeclarer topologyDeclarer,
        IServiceProvider serviceProvider,
        ILogger<RabbitMqMessagingProvider> logger)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
        _topologyDeclarer = topologyDeclarer ?? throw new ArgumentNullException(nameof(topologyDeclarer));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public string ProviderName => "RabbitMQ";

    /// <inheritdoc />
    public bool IsConnected => _connectionPool.IsConnected;

    /// <inheritdoc />
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Initializing RabbitMQ messaging provider");
        await _connectionPool.EnsureConnectedAsync(cancellationToken);
        _logger.LogInformation("RabbitMQ messaging provider initialized");
    }

    /// <inheritdoc />
    public Task<IInternalPublisher> CreatePublisherAsync(CancellationToken cancellationToken = default)
    {
        var publisher = new RabbitMqInternalPublisher(_connectionPool, _logger);
        return Task.FromResult<IInternalPublisher>(publisher);
    }

    /// <inheritdoc />
    public Task<IInternalConsumer> CreateConsumerAsync(ConsumerOptions options, CancellationToken cancellationToken = default)
    {
        var handlerInvokerRegistry = _serviceProvider.GetRequiredService<IHandlerInvokerRegistry>();
        var consumerLogger = _serviceProvider.GetRequiredService<ILogger<RabbitMqConsumer>>();

        var consumer = new RabbitMqConsumer(
            _connectionPool,
            _serviceProvider,
            handlerInvokerRegistry,
            options,
            consumerLogger);

        return Task.FromResult<IInternalConsumer>(new RabbitMqInternalConsumer(consumer, options.QueueName));
    }

    /// <inheritdoc />
    public async Task DeclareTopologyAsync(TopologyDefinition definition, CancellationToken cancellationToken = default)
    {
        await _topologyDeclarer.DeclareAsync(definition, cancellationToken);
    }

    /// <inheritdoc />
    public IHealthCheck CreateHealthCheck()
    {
        return _serviceProvider.GetRequiredService<RabbitMqHealthCheck>();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await _connectionPool.DisposeAsync();
        _logger.LogInformation("RabbitMQ messaging provider disposed");
    }
}

/// <summary>
/// Adapter that wraps RabbitMqPublisher to implement IInternalPublisher.
/// </summary>
internal sealed class RabbitMqInternalPublisher : IInternalPublisher
{
    private readonly IRabbitMqConnectionPool _connectionPool;
    private readonly ILogger _logger;

    public RabbitMqInternalPublisher(IRabbitMqConnectionPool connectionPool, ILogger logger)
    {
        _connectionPool = connectionPool;
        _logger = logger;
    }

    public async Task PublishAsync(PublishContext context, CancellationToken cancellationToken = default)
    {
        var channel = await _connectionPool.GetChannelAsync(cancellationToken);
        try
        {
            // Support raw publishing (from OutboxProcessor) where Message may be null
            var messageId = context.Message?.Id.ToString()
                ?? context.Headers.GetValueOrDefault("message-id")?.ToString()
                ?? Guid.NewGuid().ToString();

            var correlationId = context.Message?.CorrelationId
                ?? context.Headers.GetValueOrDefault("correlation-id")?.ToString();

            var properties = new global::RabbitMQ.Client.BasicProperties
            {
                Persistent = context.Persistent,
                ContentType = context.ContentType,
                MessageId = messageId,
                Timestamp = new global::RabbitMQ.Client.AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
                Headers = context.Headers.ToDictionary(x => x.Key, x => x.Value)
            };

            if (correlationId != null)
                properties.CorrelationId = correlationId;

            if (context.Priority.HasValue)
                properties.Priority = context.Priority.Value;

            if (context.TimeToLive.HasValue)
                properties.Expiration = context.TimeToLive.Value.ToString();

            await channel.BasicPublishAsync(
                exchange: context.ExchangeName ?? string.Empty,
                routingKey: context.RoutingKey ?? string.Empty,
                mandatory: false,
                basicProperties: properties,
                body: context.Body ?? [],
                cancellationToken: cancellationToken);

            _logger.LogDebug(
                "Message {MessageId} published to exchange '{Exchange}' with routing key '{RoutingKey}'",
                messageId, context.ExchangeName, context.RoutingKey);
        }
        finally
        {
            _connectionPool.ReturnChannel(channel);
        }
    }
}

/// <summary>
/// Adapter that wraps RabbitMqConsumer to implement IInternalConsumer.
/// </summary>
internal sealed class RabbitMqInternalConsumer : IInternalConsumer
{
    private readonly RabbitMqConsumer _consumer;
    private readonly string _queueName;

    public RabbitMqInternalConsumer(RabbitMqConsumer consumer, string queueName)
    {
        _consumer = consumer;
        _queueName = queueName;
    }

    public bool IsRunning => _consumer.IsRunning;

    public string SourceName => _queueName;

    public Task StartAsync(Func<Consuming.Middleware.ConsumeContext, CancellationToken, Task> handler, CancellationToken cancellationToken = default)
    {
        // The RabbitMqConsumer handles this internally through its own message processing
        return _consumer.StartAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        return _consumer.StopAsync(cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        return _consumer.DisposeAsync();
    }
}
