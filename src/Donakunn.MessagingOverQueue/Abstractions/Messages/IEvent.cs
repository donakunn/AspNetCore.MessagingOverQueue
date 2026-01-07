namespace MessagingOverQueue.src.Abstractions.Messages;

/// <summary>
/// Marker interface for events. Events represent something that has happened
/// and can be consumed by multiple subscribers.
/// </summary>
public interface IEvent : IMessage
{
}

/// <summary>
/// Base class for event messages.
/// </summary>
public abstract class Event : MessageBase, IEvent
{
}

