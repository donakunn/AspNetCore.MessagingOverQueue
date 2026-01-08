namespace Donakunn.MessagingOverQueue.Topology.Attributes;

/// <summary>
/// Marks a message type for auto-discovery by the topology scanner.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class MessageAttribute : Attribute
{
    /// <summary>
    /// Whether to include this message in topology auto-discovery. Defaults to true.
    /// </summary>
    public bool AutoDiscover { get; init; } = true;

    /// <summary>
    /// Optional group name for organizing related messages.
    /// </summary>
    public string? Group { get; init; }

    /// <summary>
    /// Optional version for the message schema.
    /// </summary>
    public string? Version { get; init; }

    /// <summary>
    /// Creates a new message attribute.
    /// </summary>
    public MessageAttribute() { }
}
