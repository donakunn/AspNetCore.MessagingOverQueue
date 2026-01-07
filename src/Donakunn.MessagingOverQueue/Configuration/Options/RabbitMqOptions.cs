namespace MessagingOverQueue.src.Configuration.Options;

/// <summary>
/// Configuration options for RabbitMQ connection.
/// </summary>
public class RabbitMqOptions
{
    /// <summary>
    /// Configuration section name.
    /// </summary>
    public const string SectionName = "RabbitMq";

    /// <summary>
    /// The RabbitMQ host name.
    /// </summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>
    /// The RabbitMQ port.
    /// </summary>
    public int Port { get; set; } = 5672;

    /// <summary>
    /// The RabbitMQ username.
    /// </summary>
    public string UserName { get; set; } = "guest";

    /// <summary>
    /// The RabbitMQ password.
    /// </summary>
    public string Password { get; set; } = "guest";

    /// <summary>
    /// The virtual host.
    /// </summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// Connection timeout.
    /// </summary>
    public TimeSpan ConnectionTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Heartbeat interval.
    /// </summary>
    public TimeSpan RequestedHeartbeat { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Whether to use SSL.
    /// </summary>
    public bool UseSsl { get; set; }

    /// <summary>
    /// SSL server name.
    /// </summary>
    public string? SslServerName { get; set; }

    /// <summary>
    /// Client-provided connection name.
    /// </summary>
    public string? ClientProvidedName { get; set; }

    /// <summary>
    /// Number of channels to pool.
    /// </summary>
    public int ChannelPoolSize { get; set; } = 10;

    /// <summary>
    /// Whether to automatically recover connections.
    /// </summary>
    public bool AutomaticRecoveryEnabled { get; set; } = true;

    /// <summary>
    /// Network recovery interval.
    /// </summary>
    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Exchange configurations.
    /// </summary>
    public List<ExchangeOptions> Exchanges { get; set; } = new();

    /// <summary>
    /// Queue configurations.
    /// </summary>
    public List<QueueOptions> Queues { get; set; } = new();

    /// <summary>
    /// Binding configurations.
    /// </summary>
    public List<BindingOptions> Bindings { get; set; } = new();
}

/// <summary>
/// Configuration for an exchange.
/// </summary>
public class ExchangeOptions
{
    /// <summary>
    /// Exchange name.
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Exchange type (direct, topic, fanout, headers).
    /// </summary>
    public string Type { get; set; } = "topic";

    /// <summary>
    /// Whether the exchange is durable.
    /// </summary>
    public bool Durable { get; set; } = true;

    /// <summary>
    /// Whether to auto-delete when no queues are bound.
    /// </summary>
    public bool AutoDelete { get; set; }

    /// <summary>
    /// Additional arguments.
    /// </summary>
    public Dictionary<string, object?>? Arguments { get; set; }
}

/// <summary>
/// Configuration for a queue.
/// </summary>
public class QueueOptions
{
    /// <summary>
    /// Queue name.
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Whether the queue is durable.
    /// </summary>
    public bool Durable { get; set; } = true;

    /// <summary>
    /// Whether the queue is exclusive.
    /// </summary>
    public bool Exclusive { get; set; }

    /// <summary>
    /// Whether to auto-delete when no consumers are connected.
    /// </summary>
    public bool AutoDelete { get; set; }

    /// <summary>
    /// Dead letter exchange name.
    /// </summary>
    public string? DeadLetterExchange { get; set; }

    /// <summary>
    /// Dead letter routing key.
    /// </summary>
    public string? DeadLetterRoutingKey { get; set; }

    /// <summary>
    /// Message time-to-live in milliseconds.
    /// </summary>
    public int? MessageTtl { get; set; }

    /// <summary>
    /// Maximum queue length.
    /// </summary>
    public int? MaxLength { get; set; }

    /// <summary>
    /// Maximum queue size in bytes.
    /// </summary>
    public long? MaxLengthBytes { get; set; }

    /// <summary>
    /// Overflow behavior (drop-head, reject-publish).
    /// </summary>
    public string? OverflowBehavior { get; set; }

    /// <summary>
    /// Additional arguments.
    /// </summary>
    public Dictionary<string, object>? Arguments { get; set; }
}

/// <summary>
/// Configuration for a binding.
/// </summary>
public class BindingOptions
{
    /// <summary>
    /// Source exchange name.
    /// </summary>
    public string Exchange { get; set; } = string.Empty;

    /// <summary>
    /// Target queue name.
    /// </summary>
    public string Queue { get; set; } = string.Empty;

    /// <summary>
    /// Routing key for the binding.
    /// </summary>
    public string RoutingKey { get; set; } = string.Empty;

    /// <summary>
    /// Additional arguments for headers exchange.
    /// </summary>
    public Dictionary<string, object?>? Arguments { get; set; }
}

