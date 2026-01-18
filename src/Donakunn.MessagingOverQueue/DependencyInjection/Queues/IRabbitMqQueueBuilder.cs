using Donakunn.MessagingOverQueue.Configuration.Builders;
using Donakunn.MessagingOverQueue.Topology.Builders;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Donakunn.MessagingOverQueue.DependencyInjection.Queues;

/// <summary>
/// Builder interface for configuring RabbitMQ queues.
/// </summary>
public interface IRabbitMqQueueBuilder : IQueueBuilder
{
    /// <summary>
    /// Configures the RabbitMQ connection options.
    /// </summary>
    /// <param name="configure">Configuration action for RabbitMQ options.</param>
    /// <returns>The builder for chaining.</returns>
    IRabbitMqQueueBuilder WithConnection(Action<RabbitMqOptionsBuilder> configure);

    /// <summary>
    /// Configures topology and handler auto-discovery.
    /// </summary>
    /// <param name="configure">Configuration action for topology options.</param>
    /// <returns>The builder for chaining.</returns>
    IRabbitMqQueueBuilder WithTopology(Action<TopologyBuilder> configure);

    /// <summary>
    /// Adds RabbitMQ health checks.
    /// </summary>
    /// <param name="name">The health check name.</param>
    /// <param name="failureStatus">The failure status.</param>
    /// <param name="tags">Optional tags.</param>
    /// <returns>The builder for chaining.</returns>
    IRabbitMqQueueBuilder WithHealthChecks(
        string name = "rabbitmq",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null);
}
