namespace Donakunn.MessagingOverQueue.Topology.Abstractions;

/// <summary>
/// Declares topology on RabbitMQ broker.
/// </summary>
public interface ITopologyDeclarer
{
    /// <summary>
    /// Declares a single topology definition on the broker.
    /// </summary>
    /// <param name="definition">The topology to declare.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task DeclareAsync(TopologyDefinition definition, CancellationToken cancellationToken = default);

    /// <summary>
    /// Declares multiple topology definitions on the broker.
    /// </summary>
    /// <param name="definitions">The topologies to declare.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task DeclareAllAsync(IEnumerable<TopologyDefinition> definitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Declares all registered topologies from the registry.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task DeclareRegisteredTopologiesAsync(CancellationToken cancellationToken = default);
}
