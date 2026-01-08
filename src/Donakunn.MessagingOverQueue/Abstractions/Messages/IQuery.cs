namespace Donakunn.MessagingOverQueue.Abstractions.Messages;

/// <summary>
/// Marker interface for query messages that expect a response.
/// </summary>
/// <typeparam name="TResult">The type of the expected result.</typeparam>
public interface IQuery<TResult> : IMessage
{
}

/// <summary>
/// Base class for query messages.
/// </summary>
/// <typeparam name="TResult">The type of the expected result.</typeparam>
public abstract class Query<TResult> : MessageBase, IQuery<TResult>
{
}

