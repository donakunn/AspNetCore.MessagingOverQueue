using Donakunn.MessagingOverQueue.Configuration.Options;
using Microsoft.Extensions.Configuration;

namespace Donakunn.MessagingOverQueue.Configuration.Sources;

/// <summary>
/// Configuration source that reads from IConfiguration (appsettings.json, environment variables, etc.).
/// Supports the standard .NET configuration system.
/// </summary>
public class AppSettingsConfigurationSource : IRabbitMqConfigurationSource
{
    private readonly IConfiguration _configuration;
    private readonly string _sectionName;

    /// <summary>
    /// Default priority for appsettings configuration (medium).
    /// </summary>
    public int Priority { get; }

    /// <summary>
    /// Creates a new appsettings configuration source.
    /// </summary>
    /// <param name="configuration">The configuration instance.</param>
    /// <param name="sectionName">The configuration section name (default: "RabbitMq").</param>
    /// <param name="priority">The priority (default: 50 for medium).</param>
    public AppSettingsConfigurationSource(
        IConfiguration configuration,
        string? sectionName = null,
        int priority = 50)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _sectionName = sectionName ?? RabbitMqOptions.SectionName;
        Priority = priority;
    }

    /// <summary>
    /// Applies configuration from appsettings.
    /// </summary>
    public void Configure(RabbitMqOptions options)
    {
        var section = _configuration.GetSection(_sectionName);
        if (!section.Exists())
        {
            return;
        }

        // Bind the section to options
        section.Bind(options);
    }
}
