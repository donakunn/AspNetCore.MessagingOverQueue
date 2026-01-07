using MessagingOverQueue.src.Configuration.Options;

namespace MessagingOverQueue.src.Configuration.Builders;

/// <summary>
/// Fluent builder for configuring queues.
/// </summary>
public class QueueBuilder
{
    private readonly QueueOptions _options = new();

    /// <summary>
    /// Sets the queue name.
    /// </summary>
    public QueueBuilder WithName(string name)
    {
        _options.Name = name;
        return this;
    }

    /// <summary>
    /// Makes the queue durable.
    /// </summary>
    public QueueBuilder Durable(bool durable = true)
    {
        _options.Durable = durable;
        return this;
    }

    /// <summary>
    /// Makes the queue exclusive.
    /// </summary>
    public QueueBuilder Exclusive(bool exclusive = true)
    {
        _options.Exclusive = exclusive;
        return this;
    }

    /// <summary>
    /// Enables auto-delete.
    /// </summary>
    public QueueBuilder AutoDelete(bool autoDelete = true)
    {
        _options.AutoDelete = autoDelete;
        return this;
    }

    /// <summary>
    /// Configures dead letter exchange.
    /// </summary>
    public QueueBuilder WithDeadLetterExchange(string exchange, string? routingKey = null)
    {
        _options.DeadLetterExchange = exchange;
        _options.DeadLetterRoutingKey = routingKey;
        return this;
    }

    /// <summary>
    /// Sets the message TTL.
    /// </summary>
    public QueueBuilder WithMessageTtl(TimeSpan ttl)
    {
        _options.MessageTtl = (int)ttl.TotalMilliseconds;
        return this;
    }

    /// <summary>
    /// Sets the maximum queue length.
    /// </summary>
    public QueueBuilder WithMaxLength(int maxLength)
    {
        _options.MaxLength = maxLength;
        return this;
    }

    /// <summary>
    /// Sets the maximum queue size in bytes.
    /// </summary>
    public QueueBuilder WithMaxLengthBytes(long maxBytes)
    {
        _options.MaxLengthBytes = maxBytes;
        return this;
    }

    /// <summary>
    /// Sets the overflow behavior to drop head.
    /// </summary>
    public QueueBuilder DropHeadOnOverflow()
    {
        _options.OverflowBehavior = "drop-head";
        return this;
    }

    /// <summary>
    /// Sets the overflow behavior to reject publish.
    /// </summary>
    public QueueBuilder RejectPublishOnOverflow()
    {
        _options.OverflowBehavior = "reject-publish";
        return this;
    }

    /// <summary>
    /// Adds an argument.
    /// </summary>
    public QueueBuilder WithArgument(string key, object value)
    {
        _options.Arguments ??= new Dictionary<string, object>();
        _options.Arguments[key] = value;
        return this;
    }

    /// <summary>
    /// Enables quorum queue type for high availability.
    /// </summary>
    public QueueBuilder AsQuorumQueue()
    {
        _options.Arguments ??= new Dictionary<string, object>();
        _options.Arguments["x-queue-type"] = "quorum";
        return this;
    }

    /// <summary>
    /// Enables stream queue type for high throughput.
    /// </summary>
    public QueueBuilder AsStreamQueue()
    {
        _options.Arguments ??= new Dictionary<string, object>();
        _options.Arguments["x-queue-type"] = "stream";
        return this;
    }

    /// <summary>
    /// Enables lazy queue mode.
    /// </summary>
    public QueueBuilder AsLazyQueue()
    {
        _options.Arguments ??= new Dictionary<string, object>();
        _options.Arguments["x-queue-mode"] = "lazy";
        return this;
    }

    /// <summary>
    /// Builds the queue options.
    /// </summary>
    public QueueOptions Build() => _options;
}

