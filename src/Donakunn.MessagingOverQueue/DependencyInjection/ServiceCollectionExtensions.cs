using Donakunn.MessagingOverQueue.Abstractions.Messages;
using Donakunn.MessagingOverQueue.Consuming.Handlers;
using Microsoft.Extensions.DependencyInjection;

namespace Donakunn.MessagingOverQueue.DependencyInjection;

/// <summary>
/// Extension methods for registering messaging services.
/// </summary>
public static class ServiceCollectionExtensions
{
}

/// <summary>
/// Builder interface for configuring messaging services.
/// </summary>
public interface IMessagingBuilder
{
    /// <summary>
    /// The service collection.
    /// </summary>
    IServiceCollection Services { get; }

    /// <summary>
    /// Indicates whether a queue provider has been configured.
    /// </summary>
    bool HasQueueProvider { get; }
}

/// <summary>
/// Default implementation of IMessagingBuilder.
/// </summary>
internal sealed class MessagingBuilder(IServiceCollection services) : IMessagingBuilder
{
    public IServiceCollection Services { get; } = services;

    /// <summary>
    /// Indicates whether a queue provider has been configured.
    /// </summary>
    public bool HasQueueProvider { get; internal set; }
}

/// <summary>
/// Interface for handler invoker registrations.
/// </summary>
public interface IHandlerInvokerRegistration
{
    /// <summary>
    /// The message type this registration is for.
    /// </summary>
    Type MessageType { get; }

    /// <summary>
    /// Registers the handler invoker with the registry.
    /// </summary>
    void Register(IHandlerInvokerRegistry registry, IHandlerInvokerFactory factory);
}

/// <summary>
/// Registration for a handler invoker.
/// </summary>
public sealed class HandlerInvokerRegistration<TMessage> : IHandlerInvokerRegistration
    where TMessage : IMessage
{
    public Type MessageType => typeof(TMessage);

    public void Register(IHandlerInvokerRegistry registry, IHandlerInvokerFactory factory)
    {
        if (!registry.IsRegistered(typeof(TMessage)))
        {
            var invoker = factory.Create(typeof(TMessage));
            registry.Register(invoker);
        }
    }
}

