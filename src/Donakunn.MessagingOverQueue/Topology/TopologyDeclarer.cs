using Donakunn.MessagingOverQueue.Connection;
using Donakunn.MessagingOverQueue.Topology.Abstractions;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Donakunn.MessagingOverQueue.Topology;

/// <summary>
/// Declares topology definitions on the RabbitMQ broker.
/// </summary>
/// <remarks>
/// Creates a new instance with the specified dependencies.
/// </remarks>
public sealed class TopologyDeclarer(
    IRabbitMqConnectionPool connectionPool,
    ITopologyRegistry registry,
    ILogger<TopologyDeclarer> logger) : ITopologyDeclarer
{
    private readonly IRabbitMqConnectionPool _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
    private readonly ITopologyRegistry _registry = registry ?? throw new ArgumentNullException(nameof(registry));
    private readonly ILogger<TopologyDeclarer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private readonly HashSet<string> _declaredExchanges = new();
    private readonly HashSet<string> _declaredQueues = new();
    private readonly Lock _lock = new();

    /// <inheritdoc />
    public async Task DeclareAsync(TopologyDefinition definition, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);

        var channel = await _connectionPool.GetChannelAsync(cancellationToken);
        try
        {
            await DeclareTopologyAsync(channel, definition, cancellationToken);
        }
        finally
        {
            _connectionPool.ReturnChannel(channel);
        }
    }

    /// <inheritdoc />
    public async Task DeclareAllAsync(IEnumerable<TopologyDefinition> definitions, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definitions);

        var channel = await _connectionPool.GetChannelAsync(cancellationToken);
        try
        {
            foreach (var definition in definitions)
            {
                await DeclareTopologyAsync(channel, definition, cancellationToken);
            }
        }
        finally
        {
            _connectionPool.ReturnChannel(channel);
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

    private async Task DeclareTopologyAsync(IChannel channel, TopologyDefinition definition, CancellationToken cancellationToken)
    {
        // Declare dead letter infrastructure first if configured
        if (definition.DeadLetter != null)
        {
            await DeclareDeadLetterAsync(channel, definition.DeadLetter, cancellationToken);
        }

        // Declare exchange
        await DeclareExchangeAsync(channel, definition.Exchange, cancellationToken);

        // Declare queue
        await DeclareQueueAsync(channel, definition.Queue, definition.DeadLetter, cancellationToken);

        // Declare binding
        await DeclareBindingAsync(channel, definition.Binding, cancellationToken);

        _logger.LogDebug(
            "Declared topology for {MessageType}: Exchange={Exchange}, Queue={Queue}, RoutingKey={RoutingKey}",
            definition.MessageType.Name,
            definition.Exchange.Name,
            definition.Queue.Name,
            definition.Binding.RoutingKey);
    }

    private async Task DeclareExchangeAsync(IChannel channel, ExchangeDefinition exchange, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (_declaredExchanges.Contains(exchange.Name))
                return;
            _declaredExchanges.Add(exchange.Name);
        }

        _logger.LogDebug("Declaring exchange '{Exchange}' of type '{Type}'", exchange.Name, exchange.Type);

        await channel.ExchangeDeclareAsync(
            exchange: exchange.Name,
            type: exchange.Type,
            durable: exchange.Durable,
            autoDelete: exchange.AutoDelete,
            arguments: exchange.Arguments,
            cancellationToken: cancellationToken);
    }

    private async Task DeclareQueueAsync(IChannel channel, QueueDefinition queue, DeadLetterDefinition? deadLetter, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (_declaredQueues.Contains(queue.Name))
                return;
            _declaredQueues.Add(queue.Name);
        }

        _logger.LogDebug("Declaring queue '{Queue}'", queue.Name);

        var arguments = new Dictionary<string, object?>();

        // Add dead letter configuration
        if (deadLetter != null)
        {
            arguments["x-dead-letter-exchange"] = deadLetter.ExchangeName;
            if (deadLetter.RoutingKey != null)
            {
                arguments["x-dead-letter-routing-key"] = deadLetter.RoutingKey;
            }
        }

        // Add queue configuration
        if (queue.MessageTtl.HasValue)
            arguments["x-message-ttl"] = queue.MessageTtl.Value;

        if (queue.MaxLength.HasValue)
            arguments["x-max-length"] = queue.MaxLength.Value;

        if (queue.MaxLengthBytes.HasValue)
            arguments["x-max-length-bytes"] = queue.MaxLengthBytes.Value;

        if (queue.OverflowBehavior != null)
            arguments["x-overflow"] = queue.OverflowBehavior;

        if (queue.QueueType != null)
            arguments["x-queue-type"] = queue.QueueType;

        // Merge additional arguments
        if (queue.Arguments != null)
        {
            foreach (var arg in queue.Arguments)
            {
                arguments[arg.Key] = arg.Value;
            }
        }

        await channel.QueueDeclareAsync(
            queue: queue.Name,
            durable: queue.Durable,
            exclusive: queue.Exclusive,
            autoDelete: queue.AutoDelete,
            arguments: arguments,
            cancellationToken: cancellationToken);
    }

    private async Task DeclareBindingAsync(IChannel channel, BindingDefinition binding, CancellationToken cancellationToken)
    {
        _logger.LogDebug(
            "Binding queue '{Queue}' to exchange '{Exchange}' with routing key '{RoutingKey}'",
            binding.QueueName, binding.ExchangeName, binding.RoutingKey);

        await channel.QueueBindAsync(
            queue: binding.QueueName,
            exchange: binding.ExchangeName,
            routingKey: binding.RoutingKey,
            arguments: binding.Arguments,
            cancellationToken: cancellationToken);
    }

    private async Task DeclareDeadLetterAsync(IChannel channel, DeadLetterDefinition deadLetter, CancellationToken cancellationToken)
    {
        // Declare dead letter exchange
        lock (_lock)
        {
            if (!_declaredExchanges.Contains(deadLetter.ExchangeName))
            {
                _declaredExchanges.Add(deadLetter.ExchangeName);
            }
            else
            {
                // Already declared, check queue
                if (_declaredQueues.Contains(deadLetter.QueueName))
                    return;
            }
        }

        _logger.LogDebug("Declaring dead letter exchange '{Exchange}'", deadLetter.ExchangeName);

        await channel.ExchangeDeclareAsync(
            exchange: deadLetter.ExchangeName,
            type: "fanout",
            durable: true,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken);

        // Declare dead letter queue
        lock (_lock)
        {
            if (_declaredQueues.Contains(deadLetter.QueueName))
                return;
            _declaredQueues.Add(deadLetter.QueueName);
        }

        _logger.LogDebug("Declaring dead letter queue '{Queue}'", deadLetter.QueueName);

        await channel.QueueDeclareAsync(
            queue: deadLetter.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken);

        // Bind dead letter queue to dead letter exchange
        await channel.QueueBindAsync(
            queue: deadLetter.QueueName,
            exchange: deadLetter.ExchangeName,
            routingKey: deadLetter.RoutingKey ?? string.Empty,
            arguments: null,
            cancellationToken: cancellationToken);
    }
}
