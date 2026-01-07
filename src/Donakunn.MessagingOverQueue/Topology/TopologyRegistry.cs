using MessagingOverQueue.src.Topology.Abstractions;
using System.Collections.Concurrent;

namespace MessagingOverQueue.src.Topology;

/// <summary>
/// Thread-safe registry for topology definitions.
/// </summary>
public sealed class TopologyRegistry : ITopologyRegistry
{
    private readonly ConcurrentDictionary<Type, TopologyDefinition> _definitions = new();

    /// <inheritdoc />
    public void Register(TopologyDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(definition.MessageType);

        _definitions.AddOrUpdate(
            definition.MessageType,
            definition,
            (_, existing) => definition);
    }

    /// <inheritdoc />
    public void RegisterRange(IEnumerable<TopologyDefinition> definitions)
    {
        ArgumentNullException.ThrowIfNull(definitions);

        foreach (var definition in definitions)
        {
            Register(definition);
        }
    }

    /// <inheritdoc />
    public TopologyDefinition? GetTopology(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        return _definitions.TryGetValue(messageType, out var definition)
            ? definition
            : null;
    }

    /// <inheritdoc />
    public TopologyDefinition? GetTopology<TMessage>()
    {
        return GetTopology(typeof(TMessage));
    }

    /// <inheritdoc />
    public IReadOnlyCollection<TopologyDefinition> GetAllTopologies()
    {
        return _definitions.Values.ToList().AsReadOnly();
    }

    /// <inheritdoc />
    public bool IsRegistered(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return _definitions.ContainsKey(messageType);
    }

    /// <inheritdoc />
    public bool IsRegistered<TMessage>()
    {
        return IsRegistered(typeof(TMessage));
    }
}
