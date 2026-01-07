using MessagingOverQueue.src.Abstractions.Consuming;
using MessagingOverQueue.src.Abstractions.Messages;
using Microsoft.Extensions.DependencyInjection;

namespace MessagingOverQueue.src.Consuming.Handlers;

/// <summary>
/// Abstraction for invoking message handlers without reflection at runtime.
/// </summary>
public interface IHandlerInvoker
{
    /// <summary>
    /// The message type this invoker handles.
    /// </summary>
    Type MessageType { get; }

    /// <summary>
    /// Invokes all registered handlers for the message.
    /// </summary>
    /// <param name="serviceProvider">The scoped service provider.</param>
    /// <param name="message">The message to handle.</param>
    /// <param name="context">The message context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task InvokeAsync(
        IServiceProvider serviceProvider,
        IMessage message,
        IMessageContext context,
        CancellationToken cancellationToken);
}

/// <summary>
/// Generic handler invoker that provides strongly-typed handler invocation.
/// </summary>
/// <typeparam name="TMessage">The message type.</typeparam>
internal sealed class HandlerInvoker<TMessage> : IHandlerInvoker
    where TMessage : IMessage
{
    public Type MessageType => typeof(TMessage);

    public async Task InvokeAsync(
        IServiceProvider serviceProvider,
        IMessage message,
        IMessageContext context,
        CancellationToken cancellationToken)
    {
        var handlers = serviceProvider.GetServices<IMessageHandler<TMessage>>();

        foreach (var handler in handlers)
        {
            await handler.HandleAsync((TMessage)message, context, cancellationToken);
        }
    }
}
