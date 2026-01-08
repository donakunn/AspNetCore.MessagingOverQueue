using Donakunn.MessagingOverQueue.Abstractions.Messages;

namespace Donakunn.MessagingOverQueue.Abstractions.Publishing;

/// <summary>
/// Interface for publishing messages to RabbitMQ.
/// </summary>
public interface IMessagePublisher
{
    /// <summary>
    /// Publishes a message to the specified exchange.
    /// </summary>
    /// <typeparam name="T">The type of message.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="exchangeName">The exchange name. If null, uses default exchange routing.</param>
    /// <param name="routingKey">The routing key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task PublishAsync<T>(T message, string? exchangeName = null, string? routingKey = null, CancellationToken cancellationToken = default) where T : IMessage;

    /// <summary>
    /// Publishes a message with custom options.
    /// </summary>
    /// <typeparam name="T">The type of message.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="options">Publishing options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task PublishAsync<T>(T message, PublishOptions options, CancellationToken cancellationToken = default) where T : IMessage;
}

/// <summary>
/// Options for publishing a message.
/// </summary>
public class PublishOptions
{
    /// <summary>
    /// The exchange name. If null, uses default exchange routing.
    /// </summary>
    public string? ExchangeName { get; set; }

    /// <summary>
    /// The routing key.
    /// </summary>
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Whether the message should be persisted.
    /// </summary>
    public bool Persistent { get; set; } = true;

    /// <summary>
    /// Message priority (0-9).
    /// </summary>
    public byte? Priority { get; set; }

    /// <summary>
    /// Message time-to-live in milliseconds.
    /// </summary>
    public int? TimeToLive { get; set; }

    /// <summary>
    /// Custom headers to include with the message.
    /// </summary>
    public Dictionary<string, object>? Headers { get; set; }

    /// <summary>
    /// Whether to wait for publisher confirms.
    /// </summary>
    public bool WaitForConfirm { get; set; } = true;

    /// <summary>
    /// Timeout for publisher confirms.
    /// </summary>
    public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromSeconds(30);
}

