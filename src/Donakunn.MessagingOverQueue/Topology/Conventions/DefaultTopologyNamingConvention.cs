using Donakunn.MessagingOverQueue.Abstractions.Messages;
using Donakunn.MessagingOverQueue.Topology.Abstractions;
using System.Text.RegularExpressions;

namespace Donakunn.MessagingOverQueue.Topology.Conventions;

/// <summary>
/// Default naming convention for RabbitMQ topology.
/// Uses message type names with consistent formatting.
/// </summary>
/// <remarks>
/// Creates a new instance with the specified options.
/// </remarks>
/// <param name="options">The naming options.</param>
public sealed partial class DefaultTopologyNamingConvention(TopologyNamingOptions options) : ITopologyNamingConvention
{
    private readonly TopologyNamingOptions _options = options ?? throw new ArgumentNullException(nameof(options));

    /// <summary>
    /// Creates a new instance with default options.
    /// </summary>
    public DefaultTopologyNamingConvention()
        : this(new TopologyNamingOptions())
    {
    }

    /// <inheritdoc />
    public string GetExchangeName(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        var baseName = GetBaseName(messageType);

        // Events use topic exchange with event-specific naming
        if (typeof(IEvent).IsAssignableFrom(messageType))
        {
            return FormatName($"{_options.EventExchangePrefix}{baseName}");
        }

        // Commands use direct exchange with command-specific naming
        if (typeof(ICommand).IsAssignableFrom(messageType))
        {
            return FormatName($"{_options.CommandExchangePrefix}{baseName}");
        }

        // Default exchange naming
        return FormatName($"{_options.DefaultExchangePrefix}{baseName}");
    }

    /// <inheritdoc />
    public string GetQueueName(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        var baseName = GetBaseName(messageType);

        if (typeof(IEvent).IsAssignableFrom(messageType))
        {
            // Events: include service name for subscriber isolation
            var serviceName = _options.ServiceName ?? "default";
            // Format individual parts before concatenation to avoid incorrect hyphen insertion
            var formattedServiceName = FormatName(serviceName);
            var formattedBaseName = FormatName(baseName);
            return $"{formattedServiceName}{_options.QueueSeparator}{formattedBaseName}";
        }

        if (typeof(ICommand).IsAssignableFrom(messageType))
        {
            // Commands: use message name directly (single consumer)
            return FormatName($"{_options.CommandQueuePrefix}{baseName}");
        }

        return FormatName($"{_options.DefaultQueuePrefix}{baseName}");
    }

    /// <summary>
    /// Gets the consumer queue name for a handler type.
    /// Uses handler-specific naming when service name is provided.
    /// </summary>
    /// <param name="handlerType">The handler type.</param>
    /// <param name="messageType">The message type being handled.</param>
    /// <returns>The consumer queue name.</returns>
    public string GetConsumerQueueName(Type handlerType, Type messageType)
    {
        ArgumentNullException.ThrowIfNull(handlerType);
        ArgumentNullException.ThrowIfNull(messageType);

        var messageBaseName = GetBaseName(messageType);
        var serviceName = _options.ServiceName ?? GetServiceNameFromHandler(handlerType);

        // Format individual parts before concatenation to avoid incorrect hyphen insertion
        var formattedServiceName = FormatName(serviceName);
        var formattedMessageName = FormatName(messageBaseName);

        return $"{formattedServiceName}{_options.QueueSeparator}{formattedMessageName}";
    }

    /// <inheritdoc />
    public string GetRoutingKey(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        var baseName = GetBaseName(messageType);

        // Use dot notation for topic exchanges
        if (typeof(IEvent).IsAssignableFrom(messageType))
        {
            var category = ExtractCategory(messageType);
            return $"{category}.{ConvertToRoutingKeySegment(baseName)}";
        }

        // Commands use simple routing key
        if (typeof(ICommand).IsAssignableFrom(messageType))
        {
            return ConvertToRoutingKeySegment(baseName);
        }

        return ConvertToRoutingKeySegment(baseName);
    }

    /// <inheritdoc />
    public string GetDeadLetterExchangeName(string sourceQueueName)
    {
        return FormatName($"{_options.DeadLetterExchangePrefix}{sourceQueueName}");
    }

    /// <inheritdoc />
    public string GetDeadLetterQueueName(string sourceQueueName)
    {
        return FormatName($"{sourceQueueName}{_options.QueueSeparator}{_options.DeadLetterQueueSuffix}");
    }

    private string GetBaseName(Type messageType)
    {
        var name = messageType.Name;

        // Remove common suffixes
        foreach (var suffix in _options.SuffixesToRemove)
        {
            if (name.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
            {
                name = name[..^suffix.Length];
                break;
            }
        }

        return name;
    }

    private static string GetServiceNameFromHandler(Type handlerType)
    {
        // Try to extract service name from namespace
        var ns = handlerType.Namespace;
        if (string.IsNullOrEmpty(ns))
            return "default";

        var parts = ns.Split('.');

        // Look for first meaningful part before common suffixes
        for (int i = 0; i < parts.Length; i++)
        {
            var part = parts[i];
            if (!part.Equals("Handlers", StringComparison.OrdinalIgnoreCase) &&
                !part.Equals("Handler", StringComparison.OrdinalIgnoreCase) &&
                !part.Equals("Services", StringComparison.OrdinalIgnoreCase) &&
                !part.Equals("src", StringComparison.OrdinalIgnoreCase))
            {
                return part.ToLowerInvariant();
            }
        }

        return "default";
    }

    private string FormatName(string name)
    {
        // Convert to kebab-case or configured format
        var formatted = _options.UseLowerCase
            ? ToKebabCase(name).ToLowerInvariant()
            : ToKebabCase(name);

        return formatted;
    }

    private static string ToKebabCase(string value)
    {
        if (string.IsNullOrEmpty(value))
            return value;

        // Insert hyphens before uppercase letters and convert
        var result = KebabCaseRegex().Replace(value, "-$1");

        // Remove leading hyphen if any
        return result.TrimStart('-').ToLowerInvariant();
    }

    private static string ConvertToRoutingKeySegment(string value)
    {
        // Convert to dot notation for routing keys
        var result = KebabCaseRegex().Replace(value, ".$1");
        return result.TrimStart('.').ToLowerInvariant();
    }

    private static string ExtractCategory(Type messageType)
    {
        // Try to extract category from namespace
        var ns = messageType.Namespace;
        if (string.IsNullOrEmpty(ns))
            return "general";

        var parts = ns.Split('.');

        // Look for common domain patterns like "Events", "Commands", "Messages"
        for (int i = parts.Length - 1; i >= 0; i--)
        {
            var part = parts[i];
            if (!part.Equals("Events", StringComparison.OrdinalIgnoreCase) &&
                !part.Equals("Commands", StringComparison.OrdinalIgnoreCase) &&
                !part.Equals("Messages", StringComparison.OrdinalIgnoreCase))
            {
                return part.ToLowerInvariant();
            }
        }

        return "general";
    }

    [GeneratedRegex(@"([A-Z])")]
    private static partial Regex KebabCaseRegex();
}

/// <summary>
/// Configuration options for topology naming.
/// </summary>
public sealed class TopologyNamingOptions
{
    /// <summary>
    /// Service name used in queue naming for event subscriptions.
    /// </summary>
    public string? ServiceName { get; set; }

    /// <summary>
    /// Whether to use lowercase names. Defaults to true.
    /// </summary>
    public bool UseLowerCase { get; set; } = true;

    /// <summary>
    /// Separator for queue name segments. Defaults to ".".
    /// </summary>
    public string QueueSeparator { get; set; } = ".";

    /// <summary>
    /// Prefix for event exchanges. Defaults to "events.".
    /// </summary>
    public string EventExchangePrefix { get; set; } = "events.";

    /// <summary>
    /// Prefix for command exchanges. Defaults to "commands.".
    /// </summary>
    public string CommandExchangePrefix { get; set; } = "commands.";

    /// <summary>
    /// Default exchange prefix. Defaults to "".
    /// </summary>
    public string DefaultExchangePrefix { get; set; } = "";

    /// <summary>
    /// Prefix for command queues. Defaults to "".
    /// </summary>
    public string CommandQueuePrefix { get; set; } = "";

    /// <summary>
    /// Default queue prefix. Defaults to "".
    /// </summary>
    public string DefaultQueuePrefix { get; set; } = "";

    /// <summary>
    /// Prefix for dead letter exchanges. Defaults to "dlx.".
    /// </summary>
    public string DeadLetterExchangePrefix { get; set; } = "dlx.";

    /// <summary>
    /// Suffix for dead letter queues. Defaults to "dlq".
    /// </summary>
    public string DeadLetterQueueSuffix { get; set; } = "dlq";

    /// <summary>
    /// Suffixes to remove from type names. Defaults to Command, Event, Message.
    /// </summary>
    public string[] SuffixesToRemove { get; set; } = ["Command", "Event", "Message", "Query"];
}