using Donakunn.MessagingOverQueue.DependencyInjection;
using Donakunn.MessagingOverQueue.DependencyInjection.Queues;
using Donakunn.MessagingOverQueue.RedisStreams.Configuration;
using Donakunn.MessagingOverQueue.Topology.Builders;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection.Queues;

/// <summary>
/// Builder interface for configuring Redis Streams queues.
/// </summary>
public interface IRedisStreamsQueueBuilder : IQueueBuilder
{
    /// <summary>
    /// Configures the Redis Streams connection options.
    /// </summary>
    /// <param name="configure">Configuration action for Redis Streams options.</param>
    /// <returns>The builder for chaining.</returns>
    IRedisStreamsQueueBuilder WithConnection(Action<RedisStreamsOptionsBuilder> configure);

    /// <summary>
    /// Configures topology and handler auto-discovery.
    /// </summary>
    /// <param name="configure">Configuration action for topology options.</param>
    /// <returns>The builder for chaining.</returns>
    IRedisStreamsQueueBuilder WithTopology(Action<TopologyBuilder> configure);

    /// <summary>
    /// Adds Redis Streams health checks.
    /// </summary>
    /// <param name="name">The health check name.</param>
    /// <param name="failureStatus">The failure status.</param>
    /// <param name="tags">Optional tags.</param>
    /// <returns>The builder for chaining.</returns>
    IRedisStreamsQueueBuilder WithHealthChecks(
        string name = "redis-streams",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null);
}
