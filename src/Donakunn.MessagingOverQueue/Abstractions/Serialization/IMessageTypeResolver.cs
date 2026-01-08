namespace Donakunn.MessagingOverQueue.Abstractions.Serialization;

/// <summary>
/// Resolves message types from type names for deserialization.
/// </summary>
public interface IMessageTypeResolver
{
    /// <summary>
    /// Registers a message type for resolution.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    void RegisterType<TMessage>();

    /// <summary>
    /// Registers a message type for resolution.
    /// </summary>
    /// <param name="messageType">The message type.</param>
    void RegisterType(Type messageType);

    /// <summary>
    /// Resolves a message type from its type name.
    /// </summary>
    /// <param name="typeName">The type name.</param>
    /// <returns>The resolved type, or null if not found.</returns>
    Type? ResolveType(string typeName);

    /// <summary>
    /// Gets all registered message types.
    /// </summary>
    /// <returns>Collection of registered types.</returns>
    IReadOnlyCollection<Type> GetRegisteredTypes();
}
