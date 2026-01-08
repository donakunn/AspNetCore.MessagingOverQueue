using Donakunn.MessagingOverQueue.Configuration.Options;

namespace Donakunn.MessagingOverQueue.Configuration.Sources;

/// <summary>
/// Represents a source of RabbitMQ configuration.
/// Implementations can provide configuration from various sources like appsettings.json, Aspire, environment variables, etc.
/// </summary>
public interface IRabbitMqConfigurationSource
{
    /// <summary>
    /// Gets the priority of this configuration source. Higher values take precedence.
    /// </summary>
    int Priority { get; }

    /// <summary>
    /// Applies configuration to the provided options.
    /// </summary>
    /// <param name="options">The options to configure.</param>
    void Configure(RabbitMqOptions options);
}
