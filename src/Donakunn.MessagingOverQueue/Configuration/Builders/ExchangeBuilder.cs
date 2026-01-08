using Donakunn.MessagingOverQueue.Configuration.Options;

namespace Donakunn.MessagingOverQueue.Configuration.Builders;

/// <summary>
/// Fluent builder for configuring exchanges.
/// </summary>
public class ExchangeBuilder
{
    private readonly ExchangeOptions _options = new();

    /// <summary>
    /// Sets the exchange name.
    /// </summary>
    public ExchangeBuilder WithName(string name)
    {
        _options.Name = name;
        return this;
    }

    /// <summary>
    /// Sets the exchange type to direct.
    /// </summary>
    public ExchangeBuilder AsDirect()
    {
        _options.Type = "direct";
        return this;
    }

    /// <summary>
    /// Sets the exchange type to topic.
    /// </summary>
    public ExchangeBuilder AsTopic()
    {
        _options.Type = "topic";
        return this;
    }

    /// <summary>
    /// Sets the exchange type to fanout.
    /// </summary>
    public ExchangeBuilder AsFanout()
    {
        _options.Type = "fanout";
        return this;
    }

    /// <summary>
    /// Sets the exchange type to headers.
    /// </summary>
    public ExchangeBuilder AsHeaders()
    {
        _options.Type = "headers";
        return this;
    }

    /// <summary>
    /// Makes the exchange durable.
    /// </summary>
    public ExchangeBuilder Durable(bool durable = true)
    {
        _options.Durable = durable;
        return this;
    }

    /// <summary>
    /// Enables auto-delete.
    /// </summary>
    public ExchangeBuilder AutoDelete(bool autoDelete = true)
    {
        _options.AutoDelete = autoDelete;
        return this;
    }

    /// <summary>
    /// Adds an argument.
    /// </summary>
    public ExchangeBuilder WithArgument(string key, object value)
    {
        _options.Arguments ??= [];
        _options.Arguments[key] = value;
        return this;
    }

    /// <summary>
    /// Builds the exchange options.
    /// </summary>
    public ExchangeOptions Build() => _options;
}

