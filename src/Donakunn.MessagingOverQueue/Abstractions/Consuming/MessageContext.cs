namespace MessagingOverQueue.src.Abstractions.Consuming;

/// <summary>
/// Default implementation of IMessageContext.
/// </summary>
public class MessageContext : IMessageContext
{
    private readonly Dictionary<string, object> _data = new();

    public MessageContext(
        Guid messageId,
        string queueName,
        string? correlationId = null,
        string? causationId = null,
        string? exchangeName = null,
        string? routingKey = null,
        IReadOnlyDictionary<string, object>? headers = null,
        int deliveryCount = 1)
    {
        MessageId = messageId;
        QueueName = queueName;
        CorrelationId = correlationId;
        CausationId = causationId;
        ExchangeName = exchangeName;
        RoutingKey = routingKey;
        Headers = headers ?? new Dictionary<string, object>();
        DeliveryCount = deliveryCount;
        ReceivedAt = DateTime.UtcNow;
    }

    public Guid MessageId { get; }
    public string? CorrelationId { get; }
    public string? CausationId { get; }
    public string QueueName { get; }
    public string? ExchangeName { get; }
    public string? RoutingKey { get; }
    public IReadOnlyDictionary<string, object> Headers { get; }
    public int DeliveryCount { get; }
    public DateTime ReceivedAt { get; }

    public void SetData<T>(string key, T value)
    {
        _data[key] = value!;
    }

    public T? GetData<T>(string key)
    {
        return _data.TryGetValue(key, out var value) ? (T)value : default;
    }
}

