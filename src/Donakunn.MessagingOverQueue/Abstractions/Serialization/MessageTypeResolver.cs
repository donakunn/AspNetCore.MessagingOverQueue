using System.Collections.Concurrent;

namespace Donakunn.MessagingOverQueue.Abstractions.Serialization;

/// <summary>
/// Default implementation of message type resolver.
/// </summary>
public sealed class MessageTypeResolver : IMessageTypeResolver
{
    private readonly ConcurrentDictionary<string, Type> _typesByName = new();
    private readonly ConcurrentDictionary<string, Type> _typesByFullName = new();
    private readonly ConcurrentDictionary<string, Type> _typesByAssemblyQualifiedName = new();
    private readonly HashSet<Type> _registeredTypes = [];
    private readonly Lock _lock = new();
    private volatile Type[]? _cachedRegisteredTypes;

    /// <inheritdoc />
    public void RegisterType<TMessage>()
    {
        RegisterType(typeof(TMessage));
    }

    /// <inheritdoc />
    public void RegisterType(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        // Fast path: use ConcurrentDictionary (thread-safe) for check
        // AssemblyQualifiedName is unique per type
        if (messageType.AssemblyQualifiedName != null &&
            _typesByAssemblyQualifiedName.ContainsKey(messageType.AssemblyQualifiedName))
            return;

        lock (_lock)
        {
            // Double-check inside lock using HashSet (only safe inside lock)
            if (_registeredTypes.Contains(messageType))
                return;

            _registeredTypes.Add(messageType);
            _cachedRegisteredTypes = null; // Invalidate cache

            // Register by simple name
            _typesByName.TryAdd(messageType.Name, messageType);

            // Register by full name
            if (messageType.FullName != null)
            {
                _typesByFullName.TryAdd(messageType.FullName, messageType);
            }

            // Register by assembly qualified name
            if (messageType.AssemblyQualifiedName != null)
            {
                _typesByAssemblyQualifiedName.TryAdd(messageType.AssemblyQualifiedName, messageType);
            }
        }
    }

    /// <inheritdoc />
    public Type? ResolveType(string typeName)
    {
        if (string.IsNullOrEmpty(typeName))
            return null;

        // Try assembly qualified name first (most specific)
        if (_typesByAssemblyQualifiedName.TryGetValue(typeName, out var type))
            return type;

        // Try full name
        if (_typesByFullName.TryGetValue(typeName, out type))
            return type;

        // Try simple name
        if (_typesByName.TryGetValue(typeName, out type))
            return type;

        // Try to load from assembly qualified name dynamically
        try
        {
            type = Type.GetType(typeName);
            if (type != null)
            {
                RegisterType(type);
                return type;
            }
        }
        catch
        {
            // Type not found or assembly not loaded
        }

        return null;
    }

    /// <inheritdoc />
    public IReadOnlyCollection<Type> GetRegisteredTypes()
    {
        var cached = _cachedRegisteredTypes;
        if (cached != null)
            return cached;

        lock (_lock)
        {
            cached = _cachedRegisteredTypes;
            if (cached != null)
                return cached;

            cached = _registeredTypes.ToArray(); // Single allocation
            _cachedRegisteredTypes = cached;
            return cached;
        }
    }
}
