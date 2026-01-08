namespace Donakunn.MessagingOverQueue.Abstractions.Consuming;

/// <summary>
/// Default implementation of IMessageContext.
/// </summary>
public class MessageContext(
    Guid messageId,
    string queueName,
    string? correlationId = null,
    string? causationId = null,
    string? exchangeName = null,
    string? routingKey = null,
    IReadOnlyDictionary<string, object>? headers = null,
    int deliveryCount = 1) : IMessageContext
{
    private readonly Dictionary<string, object> _data = new();

    public Guid MessageId { get; } = messageId;
    public string? CorrelationId { get; } = correlationId;
    public string? CausationId { get; } = causationId;
    public string QueueName { get; } = queueName;
    public string? ExchangeName { get; } = exchangeName;
    public string? RoutingKey { get; } = routingKey;
    public IReadOnlyDictionary<string, object> Headers { get; } = headers ?? new Dictionary<string, object>();
    public int DeliveryCount { get; } = deliveryCount;
    public DateTime ReceivedAt { get; } = DateTime.UtcNow;

    public void SetData<T>(string key, T value)
    {
        _data[key] = value!;
    }

    public T? GetData<T>(string key)
    {
        return _data.TryGetValue(key, out var value) ? (T)value : default;
    }
}

