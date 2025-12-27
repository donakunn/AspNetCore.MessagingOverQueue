using MessagingOverQueue.src.Abstractions.Messages;
using MessagingOverQueue.src.Topology.Abstractions;
using MessagingOverQueue.src.Topology.Attributes;
using System.Reflection;

namespace MessagingOverQueue.src.Topology;

/// <summary>
/// Convention-based topology provider that combines conventions with attribute overrides.
/// </summary>
/// <remarks>
/// Creates a new instance with the specified dependencies.
/// </remarks>
public sealed class ConventionBasedTopologyProvider(
    ITopologyNamingConvention namingConvention,
    ITopologyRegistry registry,
    TopologyProviderOptions? options = null) : ITopologyProvider
{
    private readonly ITopologyNamingConvention _namingConvention = namingConvention ?? throw new ArgumentNullException(nameof(namingConvention));
    private readonly ITopologyRegistry _registry = registry ?? throw new ArgumentNullException(nameof(registry));
    private readonly TopologyProviderOptions _options = options ?? new TopologyProviderOptions();

    /// <inheritdoc />
    public TopologyDefinition GetTopology(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        // Check registry first for cached/pre-configured definitions
        var existing = _registry.GetTopology(messageType);
        if (existing != null)
            return existing;

        // Build topology from conventions and attributes
        var definition = BuildTopologyDefinition(messageType);

        // Cache in registry
        _registry.Register(definition);

        return definition;
    }

    /// <inheritdoc />
    public IReadOnlyCollection<TopologyDefinition> GetAllTopologies()
    {
        return _registry.GetAllTopologies();
    }

    private TopologyDefinition BuildTopologyDefinition(Type messageType)
    {
        var exchangeAttr = messageType.GetCustomAttribute<ExchangeAttribute>();
        var queueAttr = messageType.GetCustomAttribute<QueueAttribute>();
        var routingKeyAttr = messageType.GetCustomAttribute<RoutingKeyAttribute>();
        var deadLetterAttr = messageType.GetCustomAttribute<DeadLetterAttribute>();

        // Determine exchange configuration
        var exchange = BuildExchangeDefinition(messageType, exchangeAttr);

        // Determine queue configuration
        var queue = BuildQueueDefinition(messageType, queueAttr, deadLetterAttr);

        // Determine routing key
        var routingKey = routingKeyAttr?.Pattern ?? _namingConvention.GetRoutingKey(messageType);

        // Build binding
        var binding = new BindingDefinition
        {
            ExchangeName = exchange.Name,
            QueueName = queue.Name,
            RoutingKey = GetBindingRoutingKey(messageType, routingKey)
        };

        // Build dead letter configuration if enabled
        var deadLetter = BuildDeadLetterDefinition(queue.Name, deadLetterAttr);

        return new TopologyDefinition
        {
            MessageType = messageType,
            Exchange = exchange,
            Queue = queue,
            Binding = binding,
            DeadLetter = deadLetter,
            RoutingKey = routingKey
        };
    }

    private ExchangeDefinition BuildExchangeDefinition(Type messageType, ExchangeAttribute? attr)
    {
        var exchangeName = attr?.Name ?? _namingConvention.GetExchangeName(messageType);
        var exchangeType = attr?.Type ?? GetDefaultExchangeType(messageType);

        return new ExchangeDefinition
        {
            Name = exchangeName,
            Type = ConvertExchangeType(exchangeType),
            Durable = attr?.Durable ?? _options.DefaultDurable,
            AutoDelete = attr?.AutoDelete ?? false
        };
    }

    private QueueDefinition BuildQueueDefinition(Type messageType, QueueAttribute? attr, DeadLetterAttribute? dlAttr)
    {
        var queueName = attr?.Name ?? _namingConvention.GetQueueName(messageType);

        var arguments = new Dictionary<string, object>();

        // Set queue type if specified
        if (attr?.QueueType is QueueType queueType && queueType != QueueType.Classic)
        {
            arguments["x-queue-type"] = ConvertQueueType(queueType);
        }

        // Set dead letter exchange if enabled
        if (dlAttr?.Enabled != false && _options.EnableDeadLetterByDefault)
        {
            var dlxName = dlAttr?.ExchangeName ?? _namingConvention.GetDeadLetterExchangeName(queueName);
            arguments["x-dead-letter-exchange"] = dlxName;

            if (dlAttr?.RoutingKey != null)
            {
                arguments["x-dead-letter-routing-key"] = dlAttr.RoutingKey;
            }
        }

        return new QueueDefinition
        {
            Name = queueName,
            Durable = attr?.Durable ?? _options.DefaultDurable,
            Exclusive = attr?.Exclusive ?? false,
            AutoDelete = attr?.AutoDelete ?? false,
            MessageTtl = attr?.MessageTtlMs > 0 ? attr?.MessageTtlMs : null,
            MaxLength = attr?.MaxLength > 0 ? attr?.MaxLength : null,
            MaxLengthBytes = attr?.MaxLengthBytes > 0 ? attr?.MaxLengthBytes : null,
            QueueType = attr?.QueueType != QueueType.Classic ? ConvertQueueType((attr?.QueueType) ?? QueueType.Classic) : null,
            Arguments = arguments.Count > 0 ? arguments : null
        };
    }

    private DeadLetterDefinition? BuildDeadLetterDefinition(string sourceQueueName, DeadLetterAttribute? attr)
    {
        // Check if dead letter is disabled
        if (attr?.Enabled == false)
            return null;

        // Check if dead letter should be enabled by default
        if (attr == null && !_options.EnableDeadLetterByDefault)
            return null;

        return new DeadLetterDefinition
        {
            ExchangeName = attr?.ExchangeName ?? _namingConvention.GetDeadLetterExchangeName(sourceQueueName),
            QueueName = attr?.QueueName ?? _namingConvention.GetDeadLetterQueueName(sourceQueueName),
            RoutingKey = attr?.RoutingKey
        };
    }

    private static ExchangeType GetDefaultExchangeType(Type messageType)
    {
        // Events use topic exchange for flexible routing
        if (typeof(IEvent).IsAssignableFrom(messageType))
            return Attributes.ExchangeType.Topic;

        // Commands use direct exchange for point-to-point
        if (typeof(ICommand).IsAssignableFrom(messageType))
            return Attributes.ExchangeType.Direct;

        // Default to topic
        return Attributes.ExchangeType.Topic;
    }

    private static string GetBindingRoutingKey(Type messageType, string routingKey)
    {
        // For events, use wildcard pattern for subscription
        if (typeof(IEvent).IsAssignableFrom(messageType))
        {
            // If routing key doesn't contain wildcards, use exact match
            if (!routingKey.Contains('*') && !routingKey.Contains('#'))
            {
                return routingKey;
            }
        }

        return routingKey;
    }

    private static string ConvertExchangeType(ExchangeType type)
    {
        return type switch
        {
            Attributes.ExchangeType.Direct => "direct",
            Attributes.ExchangeType.Topic => "topic",
            Attributes.ExchangeType.Fanout => "fanout",
            Attributes.ExchangeType.Headers => "headers",
            _ => "topic"
        };
    }

    private static string ConvertQueueType(QueueType type)
    {
        return type switch
        {
            QueueType.Quorum => "quorum",
            QueueType.Stream => "stream",
            QueueType.Lazy => "lazy",
            _ => "classic"
        };
    }
}

/// <summary>
/// Options for topology provider behavior.
/// </summary>
public sealed class TopologyProviderOptions
{
    /// <summary>
    /// Whether exchanges and queues are durable by default. Defaults to true.
    /// </summary>
    public bool DefaultDurable { get; set; } = true;

    /// <summary>
    /// Whether to enable dead letter queues by default. Defaults to true.
    /// </summary>
    public bool EnableDeadLetterByDefault { get; set; } = true;
}
