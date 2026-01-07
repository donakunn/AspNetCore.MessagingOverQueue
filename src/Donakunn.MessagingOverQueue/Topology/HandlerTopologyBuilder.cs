using MessagingOverQueue.src.Abstractions.Messages;
using MessagingOverQueue.src.Topology.Abstractions;
using MessagingOverQueue.src.Topology.Attributes;
using MessagingOverQueue.src.Topology.Builders;
using MessagingOverQueue.Topology.Conventions;

namespace MessagingOverQueue.src.Topology;

/// <summary>
/// Builds topology definitions from handler topology information.
/// Combines message-level attributes with handler-level consumer configuration.
/// </summary>
/// <remarks>
/// Creates a new instance with the specified naming convention and options.
/// </remarks>
public sealed class HandlerTopologyBuilder(
    DefaultTopologyNamingConvention namingConvention,
    TopologyProviderOptions? options = null)
{
    private readonly DefaultTopologyNamingConvention _namingConvention = namingConvention ?? throw new ArgumentNullException(nameof(namingConvention));
    private readonly TopologyProviderOptions _options = options ?? new TopologyProviderOptions();

    /// <summary>
    /// Builds a handler registration from handler topology info.
    /// </summary>
    public HandlerRegistration BuildHandlerRegistration(HandlerTopologyInfo handlerInfo)
    {
        ArgumentNullException.ThrowIfNull(handlerInfo);

        var topology = BuildTopologyDefinition(handlerInfo);
        var queueName = DetermineConsumerQueueName(handlerInfo, topology);

        return new HandlerRegistration
        {
            HandlerType = handlerInfo.HandlerType,
            MessageType = handlerInfo.MessageType,
            QueueName = queueName,
            ConsumerConfig = handlerInfo.ConsumerQueueConfig,
            TopologyDefinition = topology
        };
    }

    /// <summary>
    /// Builds a topology definition from handler topology info.
    /// </summary>
    public TopologyDefinition BuildTopologyDefinition(HandlerTopologyInfo handlerInfo)
    {
        ArgumentNullException.ThrowIfNull(handlerInfo);

        var messageType = handlerInfo.MessageType;
        var handlerType = handlerInfo.HandlerType;

        // Get attributes from message type
        var exchangeAttr = GetAttribute<ExchangeAttribute>(handlerInfo.MessageAttributes);
        var queueAttr = GetAttribute<QueueAttribute>(handlerInfo.MessageAttributes);
        var routingKeyAttr = GetAttribute<RoutingKeyAttribute>(handlerInfo.MessageAttributes);
        var deadLetterAttr = GetAttribute<DeadLetterAttribute>(handlerInfo.MessageAttributes);

        // Build exchange from message attributes
        var exchange = BuildExchangeDefinition(messageType, exchangeAttr);

        // Build queue - use handler's ConsumerQueueAttribute if present, otherwise message's QueueAttribute
        var queue = BuildQueueDefinition(messageType, handlerType, handlerInfo.ConsumerQueueConfig, queueAttr, deadLetterAttr);

        // Determine routing key
        var routingKey = routingKeyAttr?.Pattern ?? _namingConvention.GetRoutingKey(messageType);

        // Build binding
        var binding = new BindingDefinition
        {
            ExchangeName = exchange.Name,
            QueueName = queue.Name,
            RoutingKey = GetBindingRoutingKey(messageType, routingKey)
        };

        // Build dead letter configuration
        var deadLetter = BuildDeadLetterDefinition(queue.Name, deadLetterAttr, handlerInfo.ConsumerQueueConfig);

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

    private QueueDefinition BuildQueueDefinition(
        Type messageType,
        Type handlerType,
        ConsumerQueueInfo? consumerConfig,
        QueueAttribute? queueAttr,
        DeadLetterAttribute? dlAttr)
    {
        // Priority: ConsumerQueueAttribute > QueueAttribute > Convention
        string queueName;
        if (consumerConfig?.QueueName != null)
        {
            queueName = consumerConfig.QueueName;
        }
        else if (queueAttr?.Name != null)
        {
            queueName = queueAttr.Name;
        }
        else
        {
            // Use handler-aware naming convention
            queueName = _namingConvention.GetConsumerQueueName(handlerType, messageType);
        }

        var arguments = new Dictionary<string, object>();

        // Set queue type - prefer consumer config over message attribute
        var queueType = consumerConfig?.QueueType ?? (queueAttr?.QueueType != QueueType.Classic
            ? ConvertQueueType(queueAttr?.QueueType ?? QueueType.Classic)
            : null);

        if (queueType != null)
        {
            arguments["x-queue-type"] = queueType;
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
            Durable = consumerConfig?.Durable ?? queueAttr?.Durable ?? _options.DefaultDurable,
            Exclusive = consumerConfig?.Exclusive ?? queueAttr?.Exclusive ?? false,
            AutoDelete = consumerConfig?.AutoDelete ?? queueAttr?.AutoDelete ?? false,
            MessageTtl = consumerConfig?.MessageTtlMs ?? (queueAttr?.MessageTtlMs > 0 ? queueAttr.MessageTtlMs : null),
            MaxLength = consumerConfig?.MaxLength ?? (queueAttr?.MaxLength > 0 ? queueAttr.MaxLength : null),
            MaxLengthBytes = consumerConfig?.MaxLengthBytes ?? (queueAttr?.MaxLengthBytes > 0 ? queueAttr.MaxLengthBytes : null),
            QueueType = queueType,
            Arguments = arguments.Count > 0 ? arguments : null
        };
    }

    private DeadLetterDefinition? BuildDeadLetterDefinition(
        string sourceQueueName,
        DeadLetterAttribute? attr,
        ConsumerQueueInfo? consumerConfig)
    {
        // Disable dead letter for stream queues
        if (consumerConfig?.QueueType == "stream")
            return null;

        if (attr?.Enabled == false)
            return null;

        if (attr == null && !_options.EnableDeadLetterByDefault)
            return null;

        return new DeadLetterDefinition
        {
            ExchangeName = attr?.ExchangeName ?? _namingConvention.GetDeadLetterExchangeName(sourceQueueName),
            QueueName = attr?.QueueName ?? _namingConvention.GetDeadLetterQueueName(sourceQueueName),
            RoutingKey = attr?.RoutingKey
        };
    }

    private string DetermineConsumerQueueName(HandlerTopologyInfo handlerInfo, TopologyDefinition topology)
    {
        // If handler has explicit queue name, use it
        if (handlerInfo.ConsumerQueueConfig?.QueueName != null)
            return handlerInfo.ConsumerQueueConfig.QueueName;

        // Otherwise use the queue from topology
        return topology.Queue.Name;
    }

    private static ExchangeType GetDefaultExchangeType(Type messageType)
    {
        if (typeof(IEvent).IsAssignableFrom(messageType))
            return Attributes.ExchangeType.Topic;

        if (typeof(ICommand).IsAssignableFrom(messageType))
            return Attributes.ExchangeType.Direct;

        return Attributes.ExchangeType.Topic;
    }

    private static string GetBindingRoutingKey(Type messageType, string routingKey)
    {
        if (typeof(IEvent).IsAssignableFrom(messageType))
        {
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

    private static string? ConvertQueueType(QueueType type)
    {
        return type switch
        {
            QueueType.Quorum => "quorum",
            QueueType.Stream => "stream",
            QueueType.Lazy => "lazy",
            _ => null
        };
    }

    private static T? GetAttribute<T>(IReadOnlyCollection<Attribute> attributes) where T : Attribute
    {
        return attributes.OfType<T>().FirstOrDefault();
    }
}
