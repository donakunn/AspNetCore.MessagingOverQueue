using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Topology.Abstractions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Donakunn.MessagingOverQueue.Providers;

/// <summary>
/// Abstraction for a messaging provider (RabbitMQ, Redis Streams, etc.).
/// Implementations provide the core messaging infrastructure for a specific broker.
/// </summary>
public interface IMessagingProvider : IAsyncDisposable
{
    /// <summary>
    /// Gets the provider name (e.g., "RabbitMQ", "RedisStreams").
    /// </summary>
    string ProviderName { get; }

    /// <summary>
    /// Gets whether the provider is connected and ready.
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    /// Initializes the provider and establishes connection.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates an internal publisher for sending messages to the broker.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An internal publisher instance.</returns>
    Task<IInternalPublisher> CreatePublisherAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates an internal consumer for receiving messages from the broker.
    /// </summary>
    /// <param name="options">Consumer configuration options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An internal consumer instance.</returns>
    Task<IInternalConsumer> CreateConsumerAsync(ConsumerOptions options, CancellationToken cancellationToken = default);

    /// <summary>
    /// Declares topology on the broker (exchanges, queues, streams, etc.).
    /// </summary>
    /// <param name="definition">The topology definition to declare.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task DeclareTopologyAsync(TopologyDefinition definition, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a health check for this provider.
    /// </summary>
    /// <returns>A health check instance.</returns>
    IHealthCheck CreateHealthCheck();
}
