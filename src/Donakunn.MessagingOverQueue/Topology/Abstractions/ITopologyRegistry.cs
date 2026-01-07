namespace MessagingOverQueue.src.Topology.Abstractions;

/// <summary>
/// Registry that holds all discovered and configured topology definitions.
/// Thread-safe and supports runtime registration.
/// </summary>
public interface ITopologyRegistry
{
    /// <summary>
    /// Registers a topology definition for a message type.
    /// </summary>
    /// <param name="definition">The topology definition.</param>
    void Register(TopologyDefinition definition);

    /// <summary>
    /// Registers multiple topology definitions.
    /// </summary>
    /// <param name="definitions">The topology definitions.</param>
    void RegisterRange(IEnumerable<TopologyDefinition> definitions);

    /// <summary>
    /// Gets the topology definition for a message type.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    /// <returns>The topology definition, or null if not found.</returns>
    TopologyDefinition? GetTopology(Type messageType);

    /// <summary>
    /// Gets the topology definition for a message type.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <returns>The topology definition, or null if not found.</returns>
    TopologyDefinition? GetTopology<TMessage>();

    /// <summary>
    /// Gets all registered topology definitions.
    /// </summary>
    /// <returns>Collection of all topology definitions.</returns>
    IReadOnlyCollection<TopologyDefinition> GetAllTopologies();

    /// <summary>
    /// Checks if a topology is registered for a message type.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    /// <returns>True if registered; otherwise false.</returns>
    bool IsRegistered(Type messageType);

    /// <summary>
    /// Checks if a topology is registered for a message type.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <returns>True if registered; otherwise false.</returns>
    bool IsRegistered<TMessage>();
}
