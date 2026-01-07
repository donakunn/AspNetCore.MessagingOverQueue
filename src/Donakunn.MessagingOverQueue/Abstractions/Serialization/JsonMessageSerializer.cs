using MessagingOverQueue.src.Abstractions.Messages;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MessagingOverQueue.src.Abstractions.Serialization;

/// <summary>
/// JSON-based message serializer using System.Text.Json.
/// </summary>
public class JsonMessageSerializer(JsonSerializerOptions options) : IMessageSerializer
{
    private readonly JsonSerializerOptions _options = options ?? throw new ArgumentNullException(nameof(options));

    public JsonMessageSerializer() : this(CreateDefaultOptions())
    {
    }

    public string ContentType => "application/json";

    public byte[] Serialize<T>(T message) where T : IMessage
    {
        return JsonSerializer.SerializeToUtf8Bytes(message, message.GetType(), _options);
    }

    public byte[] Serialize(object message, Type type)
    {
        return JsonSerializer.SerializeToUtf8Bytes(message, type, _options);
    }

    public T? Deserialize<T>(byte[] data) where T : IMessage
    {
        return JsonSerializer.Deserialize<T>(data, _options);
    }

    public object? Deserialize(byte[] data, Type type)
    {
        return JsonSerializer.Deserialize(data, type, _options);
    }

    private static JsonSerializerOptions CreateDefaultOptions()
    {
        return new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
            WriteIndented = false,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            Converters =
            {
                new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)
            }
        };
    }
}

