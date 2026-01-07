using MessagingOverQueue.src.Abstractions.Messages;

namespace MessagingOverQueue.src.Abstractions.Serialization;

/// <summary>
/// Interface for message serialization and deserialization.
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    /// Serializes a message to a byte array.
    /// </summary>
    /// <typeparam name="T">The type of message.</typeparam>
    /// <param name="message">The message to serialize.</param>
    /// <returns>The serialized message as bytes.</returns>
    byte[] Serialize<T>(T message) where T : IMessage;

    /// <summary>
    /// Serializes a message to a byte array.
    /// </summary>
    /// <param name="message">The message to serialize.</param>
    /// <param name="type">The type of the message.</param>
    /// <returns>The serialized message as bytes.</returns>
    byte[] Serialize(object message, Type type);

    /// <summary>
    /// Deserializes a byte array to a message.
    /// </summary>
    /// <typeparam name="T">The expected type of message.</typeparam>
    /// <param name="data">The serialized message data.</param>
    /// <returns>The deserialized message.</returns>
    T? Deserialize<T>(byte[] data) where T : IMessage;

    /// <summary>
    /// Deserializes a byte array to a message.
    /// </summary>
    /// <param name="data">The serialized message data.</param>
    /// <param name="type">The type to deserialize to.</param>
    /// <returns>The deserialized message.</returns>
    object? Deserialize(byte[] data, Type type);

    /// <summary>
    /// Gets the content type for the serialization format.
    /// </summary>
    string ContentType { get; }
}

