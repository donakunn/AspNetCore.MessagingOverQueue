using Microsoft.Extensions.DependencyInjection;

namespace Donakunn.MessagingOverQueue.DependencyInjection.Queues;

/// <summary>
/// Base interface for queue provider builders.
/// </summary>
public interface IQueueBuilder
{
    /// <summary>
    /// The service collection.
    /// </summary>
    IServiceCollection Services { get; }

    /// <summary>
    /// The parent messaging builder.
    /// </summary>
    IMessagingBuilder MessagingBuilder { get; }
}
