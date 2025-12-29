using MessagingOverQueue.src.Abstractions.Messages;

namespace MessagingOverQueue.src.Abstractions.Publishing;

/// <summary>
/// Specialized interface for publishing events.
/// </summary>
public interface IEventPublisher
{
    /// <summary>
    /// Publishes an event to subscribers.
    /// </summary>
    /// <typeparam name="T">The type of event.</typeparam>
    /// <param name="event">The event to publish.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task PublishAsync<T>(T @event, CancellationToken cancellationToken = default) where T : IEvent;
}

/// <summary>
/// Specialized interface for sending commands.
/// </summary>
public interface ICommandSender
{
    /// <summary>
    /// Sends a command to a handler.
    /// </summary>
    /// <typeparam name="T">The type of command.</typeparam>
    /// <param name="command">The command to send.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SendAsync<T>(T command, CancellationToken cancellationToken = default) where T : ICommand;

    /// <summary>
    /// Sends a command to a specific queue.
    /// </summary>
    /// <typeparam name="T">The type of command.</typeparam>
    /// <param name="command">The command to send.</param>
    /// <param name="queueName">The target queue name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SendAsync<T>(T command, string queueName, CancellationToken cancellationToken = default) where T : ICommand;
}

