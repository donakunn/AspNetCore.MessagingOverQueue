using System.Reflection;
using MessagingOverQueue.Topology.Abstractions;
using MessagingOverQueue.Topology.Conventions;

namespace MessagingOverQueue.Topology.Builders;

/// <summary>
/// Fluent builder for configuring topology.
/// </summary>
public sealed class TopologyBuilder
{
    private readonly List<TopologyDefinition> _definitions = new();
    private readonly List<Assembly> _assembliesToScan = new();
    private TopologyNamingOptions _namingOptions = new();
    private TopologyProviderOptions _providerOptions = new();
    private bool _autoDiscoverEnabled = true;

    /// <summary>
    /// Configures the naming convention options.
    /// </summary>
    /// <param name="configure">Action to configure naming options.</param>
    /// <returns>The builder for chaining.</returns>
    public TopologyBuilder ConfigureNaming(Action<TopologyNamingOptions> configure)
    {
        configure(_namingOptions);
        return this;
    }

    /// <summary>
    /// Sets the service name used in queue naming.
    /// </summary>
    /// <param name="serviceName">The service name.</param>
    /// <returns>The builder for chaining.</returns>
    public TopologyBuilder WithServiceName(string serviceName)
    {
        _namingOptions.ServiceName = serviceName;
        return this;
    }

    /// <summary>
    /// Configures the topology provider options.
    /// </summary>
    /// <param name="configure">Action to configure provider options.</param>
    /// <returns>The builder for chaining.</returns>
    public TopologyBuilder ConfigureProvider(Action<TopologyProviderOptions> configure)
    {
        configure(_providerOptions);
        return this;
    }

    /// <summary>
    /// Enables or disables dead letter queues by default.
    /// </summary>
    /// <param name="enabled">Whether to enable dead letter by default.</param>
    /// <returns>The builder for chaining.</returns>
    public TopologyBuilder WithDeadLetterEnabled(bool enabled = true)
    {
        _providerOptions.EnableDeadLetterByDefault = enabled;
        return this;
    }

    /// <summary>
    /// Adds an assembly to scan for message types.
    /// </summary>
    /// <param name="assembly">The assembly to scan.</param>
    /// <returns>The builder for chaining.</returns>
    public TopologyBuilder ScanAssembly(Assembly assembly)
    {
        _assembliesToScan.Add(assembly);
        return this;
    }

    /// <summary>
    /// Adds assemblies to scan for message types.
    /// </summary>
    /// <param name="assemblies">The assemblies to scan.</param>
    /// <returns>The builder for chaining.</returns>
    public TopologyBuilder ScanAssemblies(params Assembly[] assemblies)
    {
        _assembliesToScan.AddRange(assemblies);
        return this;
    }

    /// <summary>
    /// Scans the assembly containing the specified type.
    /// </summary>
    /// <typeparam name="T">A type from the assembly to scan.</typeparam>
    /// <returns>The builder for chaining.</returns>
    public TopologyBuilder ScanAssemblyContaining<T>()
    {
        _assembliesToScan.Add(typeof(T).Assembly);
        return this;
    }

    /// <summary>
    /// Disables auto-discovery of message types.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public TopologyBuilder DisableAutoDiscovery()
    {
        _autoDiscoverEnabled = false;
        return this;
    }

    /// <summary>
    /// Adds a topology definition for a message type.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="configure">Action to configure the topology.</param>
    /// <returns>The builder for chaining.</returns>
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
    /// <param name="name">The exchange name.</param>
    /// <param name="configure">Action to configure the exchange.</param>
    /// <returns>The builder for chaining.</returns>
    public TopologyBuilder AddExchange(string name, Action<ExchangeDefinitionBuilder>? configure = null)
    {
        var builder = new ExchangeDefinitionBuilder().WithName(name);
        configure?.Invoke(builder);
        
        // Store exchange-only definition (no queue/binding)
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
/// Builder for configuring topology for a specific message type.
/// </summary>
/// <typeparam name="TMessage">The message type.</typeparam>
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

    /// <summary>
    /// Sets the exchange name.
    /// </summary>
    public ExchangeDefinitionBuilder WithName(string name)
    {
        _name = name;
        return this;
    }

    /// <summary>
    /// Sets the exchange type to direct.
    /// </summary>
    public ExchangeDefinitionBuilder AsDirect()
    {
        _type = "direct";
        return this;
    }

    /// <summary>
    /// Sets the exchange type to topic.
    /// </summary>
    public ExchangeDefinitionBuilder AsTopic()
    {
        _type = "topic";
        return this;
    }

    /// <summary>
    /// Sets the exchange type to fanout.
    /// </summary>
    public ExchangeDefinitionBuilder AsFanout()
    {
        _type = "fanout";
        return this;
    }

    /// <summary>
    /// Sets the exchange type to headers.
    /// </summary>
    public ExchangeDefinitionBuilder AsHeaders()
    {
        _type = "headers";
        return this;
    }

    /// <summary>
    /// Makes the exchange durable.
    /// </summary>
    public ExchangeDefinitionBuilder Durable(bool durable = true)
    {
        _durable = durable;
        return this;
    }

    /// <summary>
    /// Enables auto-delete.
    /// </summary>
    public ExchangeDefinitionBuilder AutoDelete(bool autoDelete = true)
    {
        _autoDelete = autoDelete;
        return this;
    }

    /// <summary>
    /// Adds an argument.
    /// </summary>
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

    /// <summary>
    /// Sets the queue name.
    /// </summary>
    public QueueDefinitionBuilder WithName(string name)
    {
        _name = name;
        return this;
    }

    /// <summary>
    /// Makes the queue durable.
    /// </summary>
    public QueueDefinitionBuilder Durable(bool durable = true)
    {
        _durable = durable;
        return this;
    }

    /// <summary>
    /// Makes the queue exclusive.
    /// </summary>
    public QueueDefinitionBuilder Exclusive(bool exclusive = true)
    {
        _exclusive = exclusive;
        return this;
    }

    /// <summary>
    /// Enables auto-delete.
    /// </summary>
    public QueueDefinitionBuilder AutoDelete(bool autoDelete = true)
    {
        _autoDelete = autoDelete;
        return this;
    }

    /// <summary>
    /// Sets the message TTL.
    /// </summary>
    public QueueDefinitionBuilder WithMessageTtl(TimeSpan ttl)
    {
        _messageTtl = (int)ttl.TotalMilliseconds;
        return this;
    }

    /// <summary>
    /// Sets the maximum queue length.
    /// </summary>
    public QueueDefinitionBuilder WithMaxLength(int maxLength)
    {
        _maxLength = maxLength;
        return this;
    }

    /// <summary>
    /// Sets the maximum queue size in bytes.
    /// </summary>
    public QueueDefinitionBuilder WithMaxLengthBytes(long maxBytes)
    {
        _maxLengthBytes = maxBytes;
        return this;
    }

    /// <summary>
    /// Configures as quorum queue.
    /// </summary>
    public QueueDefinitionBuilder AsQuorumQueue()
    {
        _queueType = "quorum";
        return this;
    }

    /// <summary>
    /// Configures as stream queue.
    /// </summary>
    public QueueDefinitionBuilder AsStreamQueue()
    {
        _queueType = "stream";
        return this;
    }

    /// <summary>
    /// Configures as lazy queue.
    /// </summary>
    public QueueDefinitionBuilder AsLazyQueue()
    {
        _queueType = "lazy";
        return this;
    }

    /// <summary>
    /// Adds an argument.
    /// </summary>
    public QueueDefinitionBuilder WithArgument(string key, object value)
    {
        _arguments ??= new Dictionary<string, object>();
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

    /// <summary>
    /// Sets the dead letter exchange name.
    /// </summary>
    public DeadLetterDefinitionBuilder WithExchange(string name)
    {
        _exchangeName = name;
        return this;
    }

    /// <summary>
    /// Sets the dead letter queue name.
    /// </summary>
    public DeadLetterDefinitionBuilder WithQueue(string name)
    {
        _queueName = name;
        return this;
    }

    /// <summary>
    /// Sets the dead letter routing key.
    /// </summary>
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
