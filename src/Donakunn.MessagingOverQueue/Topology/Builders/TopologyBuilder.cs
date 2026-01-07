using MessagingOverQueue.src.Topology.Abstractions;
using MessagingOverQueue.Topology.Conventions;
using System.Reflection;

namespace MessagingOverQueue.src.Topology.Builders;

/// <summary>
/// Fluent builder for configuring topology with handler-based auto-discovery.
/// Scans assemblies for message handlers and automatically configures topology.
/// </summary>
public sealed class TopologyBuilder
{
    private readonly List<TopologyDefinition> _definitions = [];
    private readonly List<Assembly> _assembliesToScan = [];
    private TopologyNamingOptions _namingOptions = new();
    private TopologyProviderOptions _providerOptions = new();
    private bool _autoDiscoverEnabled = true;

    /// <summary>
    /// Configures the naming convention options.
    /// </summary>
    public TopologyBuilder ConfigureNaming(Action<TopologyNamingOptions> configure)
    {
        configure(_namingOptions);
        return this;
    }

    /// <summary>
    /// Sets the service name used in queue naming.
    /// </summary>
    public TopologyBuilder WithServiceName(string serviceName)
    {
        _namingOptions.ServiceName = serviceName;
        return this;
    }

    /// <summary>
    /// Configures the topology provider options.
    /// </summary>
    public TopologyBuilder ConfigureProvider(Action<TopologyProviderOptions> configure)
    {
        configure(_providerOptions);
        return this;
    }

    /// <summary>
    /// Enables or disables dead letter queues by default.
    /// </summary>
    public TopologyBuilder WithDeadLetterEnabled(bool enabled = true)
    {
        _providerOptions.EnableDeadLetterByDefault = enabled;
        return this;
    }

    /// <summary>
    /// Adds an assembly to scan for message handlers.
    /// </summary>
    public TopologyBuilder ScanAssembly(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        if (!_assembliesToScan.Contains(assembly))
        {
            _assembliesToScan.Add(assembly);
        }
        return this;
    }

    /// <summary>
    /// Adds assemblies to scan for message handlers.
    /// </summary>
    public TopologyBuilder ScanAssemblies(params Assembly[] assemblies)
    {
        foreach (var assembly in assemblies)
        {
            ScanAssembly(assembly);
        }
        return this;
    }

    /// <summary>
    /// Scans the assembly containing the specified type for message handlers.
    /// </summary>
    public TopologyBuilder ScanAssemblyContaining<T>()
    {
        return ScanAssembly(typeof(T).Assembly);
    }

    /// <summary>
    /// Disables auto-discovery of message handlers.
    /// Use this when manually configuring all handlers and topology.
    /// </summary>
    public TopologyBuilder DisableAutoDiscovery()
    {
        _autoDiscoverEnabled = false;
        return this;
    }

    /// <summary>
    /// Adds a topology definition for a message type manually.
    /// </summary>
    public TopologyBuilder AddTopology<TMessage>(Action<MessageTopologyBuilder<TMessage>> configure)
    {
        var messageBuilder = new MessageTopologyBuilder<TMessage>(_namingOptions);
        configure(messageBuilder);
        _definitions.Add(messageBuilder.Build());
        return this;
    }

    /// <summary>
    /// Adds a custom exchange.
    /// </summary>
    public TopologyBuilder AddExchange(string name, Action<ExchangeDefinitionBuilder>? configure = null)
    {
        var builder = new ExchangeDefinitionBuilder().WithName(name);
        configure?.Invoke(builder);

        _definitions.Add(new TopologyDefinition
        {
            MessageType = typeof(object),
            Exchange = builder.Build(),
            Queue = new QueueDefinition { Name = string.Empty },
            Binding = new BindingDefinition()
        });

        return this;
    }

    /// <summary>
    /// Gets the configured naming options.
    /// </summary>
    public TopologyNamingOptions NamingOptions => _namingOptions;

    /// <summary>
    /// Gets the configured provider options.
    /// </summary>
    public TopologyProviderOptions ProviderOptions => _providerOptions;

    /// <summary>
    /// Gets the assemblies to scan.
    /// </summary>
    public IReadOnlyList<Assembly> AssembliesToScan => _assembliesToScan;

    /// <summary>
    /// Gets the manually configured definitions.
    /// </summary>
    public IReadOnlyList<TopologyDefinition> Definitions => _definitions;

    /// <summary>
    /// Gets whether auto-discovery is enabled.
    /// </summary>
    public bool AutoDiscoverEnabled => _autoDiscoverEnabled;
}

/// <summary>
/// Represents a handler registration with its topology configuration.
/// </summary>
public sealed class HandlerRegistration
{
    /// <summary>
    /// The handler type.
    /// </summary>
    public Type HandlerType { get; init; } = null!;

    /// <summary>
    /// The message type this handler handles.
    /// </summary>
    public Type MessageType { get; init; } = null!;

    /// <summary>
    /// The queue name for this handler's consumer.
    /// </summary>
    public string QueueName { get; init; } = string.Empty;

    /// <summary>
    /// Consumer configuration options.
    /// </summary>
    public ConsumerQueueInfo? ConsumerConfig { get; init; }

    /// <summary>
    /// The topology definition for this handler.
    /// </summary>
    public TopologyDefinition? TopologyDefinition { get; init; }
}

/// <summary>
/// Builder for configuring topology for a specific message type.
/// </summary>
public sealed class MessageTopologyBuilder<TMessage>
{
    private readonly TopologyNamingOptions _namingOptions;
    private readonly DefaultTopologyNamingConvention _namingConvention;

    private string? _exchangeName;
    private string _exchangeType = "topic";
    private bool _exchangeDurable = true;
    private bool _exchangeAutoDelete;

    private string? _queueName;
    private bool _queueDurable = true;
    private bool _queueExclusive;
    private bool _queueAutoDelete;
    private int? _messageTtl;
    private int? _maxLength;
    private string? _queueType;

    private string? _routingKey;

    private bool _deadLetterEnabled = true;
    private string? _deadLetterExchange;
    private string? _deadLetterQueue;
    private string? _deadLetterRoutingKey;

    internal MessageTopologyBuilder(TopologyNamingOptions namingOptions)
    {
        _namingOptions = namingOptions;
        _namingConvention = new DefaultTopologyNamingConvention(namingOptions);
    }

    /// <summary>
    /// Configures the exchange.
    /// </summary>
    public MessageTopologyBuilder<TMessage> WithExchange(Action<ExchangeDefinitionBuilder> configure)
    {
        var builder = new ExchangeDefinitionBuilder();
        configure(builder);
        var exchange = builder.Build();

        _exchangeName = exchange.Name;
        _exchangeType = exchange.Type;
        _exchangeDurable = exchange.Durable;
        _exchangeAutoDelete = exchange.AutoDelete;

        return this;
    }

    /// <summary>
    /// Sets the exchange name.
    /// </summary>
    public MessageTopologyBuilder<TMessage> WithExchangeName(string name)
    {
        _exchangeName = name;
        return this;
    }

    /// <summary>
    /// Configures the queue.
    /// </summary>
    public MessageTopologyBuilder<TMessage> WithQueue(Action<QueueDefinitionBuilder> configure)
    {
        var builder = new QueueDefinitionBuilder();
        configure(builder);
        var queue = builder.Build();

        _queueName = queue.Name;
        _queueDurable = queue.Durable;
        _queueExclusive = queue.Exclusive;
        _queueAutoDelete = queue.AutoDelete;
        _messageTtl = queue.MessageTtl;
        _maxLength = queue.MaxLength;
        _queueType = queue.QueueType;

        return this;
    }

    /// <summary>
    /// Sets the queue name.
    /// </summary>
    public MessageTopologyBuilder<TMessage> WithQueueName(string name)
    {
        _queueName = name;
        return this;
    }

    /// <summary>
    /// Sets the routing key.
    /// </summary>
    public MessageTopologyBuilder<TMessage> WithRoutingKey(string routingKey)
    {
        _routingKey = routingKey;
        return this;
    }

    /// <summary>
    /// Configures dead letter handling.
    /// </summary>
    public MessageTopologyBuilder<TMessage> WithDeadLetter(Action<DeadLetterDefinitionBuilder>? configure = null)
    {
        _deadLetterEnabled = true;
        if (configure != null)
        {
            var builder = new DeadLetterDefinitionBuilder();
            configure(builder);
            var dl = builder.Build();

            _deadLetterExchange = dl.ExchangeName;
            _deadLetterQueue = dl.QueueName;
            _deadLetterRoutingKey = dl.RoutingKey;
        }
        return this;
    }

    /// <summary>
    /// Disables dead letter handling.
    /// </summary>
    public MessageTopologyBuilder<TMessage> WithoutDeadLetter()
    {
        _deadLetterEnabled = false;
        return this;
    }

    internal TopologyDefinition Build()
    {
        var messageType = typeof(TMessage);

        var exchangeName = _exchangeName ?? _namingConvention.GetExchangeName(messageType);
        var queueName = _queueName ?? _namingConvention.GetQueueName(messageType);
        var routingKey = _routingKey ?? _namingConvention.GetRoutingKey(messageType);

        var exchange = new ExchangeDefinition
        {
            Name = exchangeName,
            Type = _exchangeType,
            Durable = _exchangeDurable,
            AutoDelete = _exchangeAutoDelete
        };

        var queue = new QueueDefinition
        {
            Name = queueName,
            Durable = _queueDurable,
            Exclusive = _queueExclusive,
            AutoDelete = _queueAutoDelete,
            MessageTtl = _messageTtl,
            MaxLength = _maxLength,
            QueueType = _queueType
        };

        var binding = new BindingDefinition
        {
            ExchangeName = exchangeName,
            QueueName = queueName,
            RoutingKey = routingKey
        };

        DeadLetterDefinition? deadLetter = null;
        if (_deadLetterEnabled)
        {
            deadLetter = new DeadLetterDefinition
            {
                ExchangeName = _deadLetterExchange ?? _namingConvention.GetDeadLetterExchangeName(queueName),
                QueueName = _deadLetterQueue ?? _namingConvention.GetDeadLetterQueueName(queueName),
                RoutingKey = _deadLetterRoutingKey
            };
        }

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
}

/// <summary>
/// Builder for exchange definitions.
/// </summary>
public sealed class ExchangeDefinitionBuilder
{
    private string _name = string.Empty;
    private string _type = "topic";
    private bool _durable = true;
    private bool _autoDelete;
    private Dictionary<string, object?>? _arguments;

    public ExchangeDefinitionBuilder WithName(string name)
    {
        _name = name;
        return this;
    }

    public ExchangeDefinitionBuilder AsDirect()
    {
        _type = "direct";
        return this;
    }

    public ExchangeDefinitionBuilder AsTopic()
    {
        _type = "topic";
        return this;
    }

    public ExchangeDefinitionBuilder AsFanout()
    {
        _type = "fanout";
        return this;
    }

    public ExchangeDefinitionBuilder AsHeaders()
    {
        _type = "headers";
        return this;
    }

    public ExchangeDefinitionBuilder Durable(bool durable = true)
    {
        _durable = durable;
        return this;
    }

    public ExchangeDefinitionBuilder AutoDelete(bool autoDelete = true)
    {
        _autoDelete = autoDelete;
        return this;
    }

    public ExchangeDefinitionBuilder WithArgument(string key, object value)
    {
        _arguments ??= [];
        _arguments[key] = value;
        return this;
    }

    internal ExchangeDefinition Build() => new()
    {
        Name = _name,
        Type = _type,
        Durable = _durable,
        AutoDelete = _autoDelete,
        Arguments = _arguments
    };
}

/// <summary>
/// Builder for queue definitions.
/// </summary>
public sealed class QueueDefinitionBuilder
{
    private string _name = string.Empty;
    private bool _durable = true;
    private bool _exclusive;
    private bool _autoDelete;
    private int? _messageTtl;
    private int? _maxLength;
    private long? _maxLengthBytes;
    private string? _queueType;
    private Dictionary<string, object>? _arguments;

    public QueueDefinitionBuilder WithName(string name)
    {
        _name = name;
        return this;
    }

    public QueueDefinitionBuilder Durable(bool durable = true)
    {
        _durable = durable;
        return this;
    }

    public QueueDefinitionBuilder Exclusive(bool exclusive = true)
    {
        _exclusive = exclusive;
        return this;
    }

    public QueueDefinitionBuilder AutoDelete(bool autoDelete = true)
    {
        _autoDelete = autoDelete;
        return this;
    }

    public QueueDefinitionBuilder WithMessageTtl(TimeSpan ttl)
    {
        _messageTtl = (int)ttl.TotalMilliseconds;
        return this;
    }

    public QueueDefinitionBuilder WithMaxLength(int maxLength)
    {
        _maxLength = maxLength;
        return this;
    }

    public QueueDefinitionBuilder WithMaxLengthBytes(long maxBytes)
    {
        _maxLengthBytes = maxBytes;
        return this;
    }

    public QueueDefinitionBuilder AsQuorumQueue()
    {
        _queueType = "quorum";
        return this;
    }

    public QueueDefinitionBuilder AsStreamQueue()
    {
        _queueType = "stream";
        return this;
    }

    public QueueDefinitionBuilder AsLazyQueue()
    {
        _queueType = "lazy";
        return this;
    }

    public QueueDefinitionBuilder WithArgument(string key, object value)
    {
        _arguments ??= [];
        _arguments[key] = value;
        return this;
    }

    internal QueueDefinition Build() => new()
    {
        Name = _name,
        Durable = _durable,
        Exclusive = _exclusive,
        AutoDelete = _autoDelete,
        MessageTtl = _messageTtl,
        MaxLength = _maxLength,
        MaxLengthBytes = _maxLengthBytes,
        QueueType = _queueType,
        Arguments = _arguments
    };
}

/// <summary>
/// Builder for dead letter definitions.
/// </summary>
public sealed class DeadLetterDefinitionBuilder
{
    private string? _exchangeName;
    private string? _queueName;
    private string? _routingKey;

    public DeadLetterDefinitionBuilder WithExchange(string name)
    {
        _exchangeName = name;
        return this;
    }

    public DeadLetterDefinitionBuilder WithQueue(string name)
    {
        _queueName = name;
        return this;
    }

    public DeadLetterDefinitionBuilder WithRoutingKey(string routingKey)
    {
        _routingKey = routingKey;
        return this;
    }

    internal DeadLetterDefinition Build() => new()
    {
        ExchangeName = _exchangeName ?? string.Empty,
        QueueName = _queueName ?? string.Empty,
        RoutingKey = _routingKey
    };
}
