namespace Donakunn.MessagingOverQueue.Abstractions.Messages;

/// <summary>
/// Marker interface for commands. Commands represent intent to change state
/// and are typically handled by a single consumer.
/// </summary>
public interface ICommand : IMessage
{
}

/// <summary>
/// Base class for command messages.
/// </summary>
public abstract class Command : MessageBase, ICommand
{
}

