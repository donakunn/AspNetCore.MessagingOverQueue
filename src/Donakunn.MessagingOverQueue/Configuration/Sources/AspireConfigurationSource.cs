using Donakunn.MessagingOverQueue.Configuration.Options;
using Microsoft.Extensions.Configuration;

namespace Donakunn.MessagingOverQueue.Configuration.Sources;

/// <summary>
/// Configuration source that integrates with .NET Aspire service discovery and configuration.
/// Automatically discovers RabbitMQ connection information from Aspire.
/// </summary>
public class AspireConfigurationSource : IRabbitMqConfigurationSource
{
    private readonly IConfiguration _configuration;
    private readonly string _connectionName;

    /// <summary>
    /// Default priority for Aspire configuration (low, as it provides defaults).
    /// </summary>
    public int Priority { get; }

    /// <summary>
    /// Creates a new Aspire configuration source.
    /// </summary>
    /// <param name="configuration">The configuration instance.</param>
    /// <param name="connectionName">The connection name in Aspire (default: "rabbitmq").</param>
    /// <param name="priority">The priority (default: 25 for low).</param>
    public AspireConfigurationSource(
        IConfiguration configuration,
        string connectionName = "rabbitmq",
        int priority = 25)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _connectionName = connectionName ?? throw new ArgumentNullException(nameof(connectionName));
        Priority = priority;
    }

    /// <summary>
    /// Applies configuration from Aspire service discovery.
    /// </summary>
    public void Configure(RabbitMqOptions options)
    {
        // Try to get connection string from Aspire
        var connectionString = _configuration.GetConnectionString(_connectionName);
        if (string.IsNullOrEmpty(connectionString))
        {
            return;
        }

        // Parse Aspire connection string (format: amqp://user:pass@host:port/vhost)
        if (TryParseConnectionString(connectionString, out var parsedOptions))
        {
            ApplyParsedOptions(parsedOptions, options);
        }

        // Also check for Aspire-specific configuration section
        var aspireSection = _configuration.GetSection($"Aspire:RabbitMQ:{_connectionName}");
        if (aspireSection.Exists())
        {
            aspireSection.Bind(options);
        }
    }

    private static bool TryParseConnectionString(string connectionString, out RabbitMqOptions options)
    {
        options = new RabbitMqOptions();

        try
        {
            // Support both amqp:// and rabbitmq:// schemes
            if (!connectionString.StartsWith("amqp://", StringComparison.OrdinalIgnoreCase) &&
                !connectionString.StartsWith("amqps://", StringComparison.OrdinalIgnoreCase) &&
                !connectionString.StartsWith("rabbitmq://", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            var uri = new Uri(connectionString);

            options.HostName = uri.Host;
            options.Port = uri.Port > 0 ? uri.Port : (uri.Scheme == "amqps" ? 5671 : 5672);
            options.UseSsl = uri.Scheme == "amqps";

            if (!string.IsNullOrEmpty(uri.UserInfo))
            {
                var userInfo = uri.UserInfo.Split(':');
                if (userInfo.Length > 0)
                {
                    options.UserName = Uri.UnescapeDataString(userInfo[0]);
                }
                if (userInfo.Length > 1)
                {
                    options.Password = Uri.UnescapeDataString(userInfo[1]);
                }
            }

            if (!string.IsNullOrEmpty(uri.AbsolutePath) && uri.AbsolutePath != "/")
            {
                options.VirtualHost = Uri.UnescapeDataString(uri.AbsolutePath.TrimStart('/'));
            }

            return true;
        }
        catch
        {
            return false;
        }
    }

    private static void ApplyParsedOptions(RabbitMqOptions source, RabbitMqOptions target)
    {
        target.HostName = source.HostName;
        target.Port = source.Port;
        target.UserName = source.UserName;
        target.Password = source.Password;
        target.VirtualHost = source.VirtualHost;
        target.UseSsl = source.UseSsl;
    }
}
