namespace Donakunn.MessagingOverQueue.Abstractions.Messages;

/// <summary>
/// Marker interface for commands. Commands represent intent to change state
/// and are typically handled by a single consumer.
/// </summary>
public interface ICommand : IMessage
{
}

/// <summary>
/// Base record for command messages.
/// </summary>
public abstract record Command : MessageBase, ICommand
{
}

