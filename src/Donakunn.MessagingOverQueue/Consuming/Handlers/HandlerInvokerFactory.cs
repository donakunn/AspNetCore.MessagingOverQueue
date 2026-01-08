using Donakunn.MessagingOverQueue.Abstractions.Messages;

namespace Donakunn.MessagingOverQueue.Consuming.Handlers;

/// <summary>
/// Factory for creating handler invokers using generic type activation.
/// This approach eliminates runtime reflection during message handling.
/// </summary>
public interface IHandlerInvokerFactory
{
    /// <summary>
    /// Creates a handler invoker for the specified message type.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    /// <returns>A handler invoker instance.</returns>
    IHandlerInvoker Create(Type messageType);
}

/// <summary>
/// Default implementation that creates strongly-typed invokers at registration time.
/// </summary>
public sealed class HandlerInvokerFactory : IHandlerInvokerFactory
{
    private static readonly Type HandlerInvokerOpenGeneric = typeof(HandlerInvoker<>);

    /// <inheritdoc />
    public IHandlerInvoker Create(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        if (!typeof(IMessage).IsAssignableFrom(messageType))
        {
            throw new ArgumentException(
                $"Type '{messageType.FullName}' does not implement IMessage.",
                nameof(messageType));
        }

        // Create HandlerInvoker<TMessage> at registration time (startup)
        // This is the only reflection cost - paid once during startup
        var invokerType = HandlerInvokerOpenGeneric.MakeGenericType(messageType);
        return (IHandlerInvoker)Activator.CreateInstance(invokerType)!;
    }
}
