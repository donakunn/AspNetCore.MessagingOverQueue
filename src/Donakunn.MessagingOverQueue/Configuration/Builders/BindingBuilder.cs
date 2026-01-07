using MessagingOverQueue.src.Configuration.Options;

namespace MessagingOverQueue.src.Configuration.Builders;

/// <summary>
/// Fluent builder for configuring bindings.
/// </summary>
public class BindingBuilder
{
    private readonly BindingOptions _options = new();

    /// <summary>
    /// Sets the source exchange.
    /// </summary>
    public BindingBuilder FromExchange(string exchangeName)
    {
        _options.Exchange = exchangeName;
        return this;
    }

    /// <summary>
    /// Sets the target queue.
    /// </summary>
    public BindingBuilder ToQueue(string queueName)
    {
        _options.Queue = queueName;
        return this;
    }

    /// <summary>
    /// Sets the routing key.
    /// </summary>
    public BindingBuilder WithRoutingKey(string routingKey)
    {
        _options.RoutingKey = routingKey;
        return this;
    }

    /// <summary>
    /// Adds an argument for headers exchange.
    /// </summary>
    public BindingBuilder WithArgument(string key, object value)
    {
        _options.Arguments ??= [];
        _options.Arguments[key] = value;
        return this;
    }

    /// <summary>
    /// Sets x-match to all for headers exchange.
    /// </summary>
    public BindingBuilder MatchAll()
    {
        _options.Arguments ??= [];
        _options.Arguments["x-match"] = "all";
        return this;
    }

    /// <summary>
    /// Sets x-match to any for headers exchange.
    /// </summary>
    public BindingBuilder MatchAny()
    {
        _options.Arguments ??= [];
        _options.Arguments["x-match"] = "any";
        return this;
    }

    /// <summary>
    /// Builds the binding options.
    /// </summary>
    public BindingOptions Build() => _options;
}

