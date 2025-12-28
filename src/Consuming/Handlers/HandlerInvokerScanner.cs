using MessagingOverQueue.src.Abstractions.Consuming;
using MessagingOverQueue.src.Abstractions.Messages;
using MessagingOverQueue.src.Topology;
using MessagingOverQueue.src.Topology.Abstractions;
using Microsoft.Extensions.Logging;
using System.Reflection;

namespace MessagingOverQueue.src.Consuming.Handlers;

/// <summary>
/// Scans assemblies for message handlers and registers invokers at startup.
/// Follows the same pattern as TopologyScanner for consistency.
/// </summary>
public interface IHandlerInvokerScanner
{
    /// <summary>
    /// Scans assemblies and registers handler invokers for all discovered handlers.
    /// </summary>
    /// <param name="assemblies">The assemblies to scan.</param>
    void ScanAndRegister(params Assembly[] assemblies);

    /// <summary>
    /// Registers an invoker for a specific message type.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    void RegisterInvoker<TMessage>() where TMessage : IMessage;

    /// <summary>
    /// Registers an invoker for a specific message type.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    void RegisterInvoker(Type messageType);
}

/// <summary>
/// Default implementation that scans for handlers and creates invokers at startup.
/// </summary>
public sealed class HandlerInvokerScanner : IHandlerInvokerScanner
{
    private readonly IHandlerInvokerRegistry _registry;
    private readonly IHandlerInvokerFactory _factory;
    private readonly ITopologyScanner _topologyScanner;
    private readonly ILogger<HandlerInvokerScanner> _logger;

    public HandlerInvokerScanner(
        IHandlerInvokerRegistry registry,
        IHandlerInvokerFactory factory,
        ITopologyScanner topologyScanner,
        ILogger<HandlerInvokerScanner> logger)
    {
        _registry = registry;
        _factory = factory;
        _topologyScanner = topologyScanner;
        _logger = logger;
    }

    /// <inheritdoc />
    public void ScanAndRegister(params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);

        _logger.LogInformation("Scanning {Count} assemblies for message handlers", assemblies.Length);

        // Leverage the existing TopologyScanner to find handlers
        var handlers = _topologyScanner.ScanForHandlers(assemblies);

        foreach (var handlerInfo in handlers)
        {
            RegisterInvoker(handlerInfo.MessageType);
        }

        _logger.LogInformation("Registered {Count} handler invokers", handlers.Count);
    }

    /// <inheritdoc />
    public void RegisterInvoker<TMessage>() where TMessage : IMessage
    {
        RegisterInvoker(typeof(TMessage));
    }

    /// <inheritdoc />
    public void RegisterInvoker(Type messageType)
    {
        if (_registry.IsRegistered(messageType))
        {
            _logger.LogDebug("Handler invoker for {MessageType} already registered", messageType.Name);
            return;
        }

        var invoker = _factory.Create(messageType);
        _registry.Register(invoker);

        _logger.LogDebug("Registered handler invoker for {MessageType}", messageType.Name);
    }
}
