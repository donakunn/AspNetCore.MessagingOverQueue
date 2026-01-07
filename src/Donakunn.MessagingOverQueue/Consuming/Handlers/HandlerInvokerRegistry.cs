using MessagingOverQueue.src.Abstractions.Messages;
using System.Collections.Concurrent;

namespace MessagingOverQueue.src.Consuming.Handlers;

/// <summary>
/// Registry for handler invokers, providing O(1) lookup by message type.
/// Thread-safe and optimized for high-throughput message processing.
/// </summary>
public interface IHandlerInvokerRegistry
{
    /// <summary>
    /// Gets the handler invoker for the specified message type.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    /// <returns>The handler invoker, or null if not registered.</returns>
    IHandlerInvoker? GetInvoker(Type messageType);

    /// <summary>
    /// Registers a handler invoker for a message type.
    /// </summary>
    /// <param name="invoker">The handler invoker.</param>
    void Register(IHandlerInvoker invoker);

    /// <summary>
    /// Checks if a handler is registered for the message type.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    bool IsRegistered(Type messageType);

    /// <summary>
    /// Gets all registered message types.
    /// </summary>
    IEnumerable<Type> GetRegisteredMessageTypes();
}

/// <summary>
/// Thread-safe implementation of handler invoker registry.
/// Uses a concurrent dictionary for O(1) lookups during message processing.
/// </summary>
public sealed class HandlerInvokerRegistry : IHandlerInvokerRegistry
{
    private readonly ConcurrentDictionary<Type, IHandlerInvoker> _invokers = new();

    /// <inheritdoc />
    public IHandlerInvoker? GetInvoker(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return _invokers.TryGetValue(messageType, out var invoker) ? invoker : null;
    }

    /// <inheritdoc />
    public void Register(IHandlerInvoker invoker)
    {
        ArgumentNullException.ThrowIfNull(invoker);
        _invokers.TryAdd(invoker.MessageType, invoker);
    }

    /// <inheritdoc />
    public bool IsRegistered(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return _invokers.ContainsKey(messageType);
    }

    /// <inheritdoc />
    public IEnumerable<Type> GetRegisteredMessageTypes()
    {
        return _invokers.Keys;
    }
}
