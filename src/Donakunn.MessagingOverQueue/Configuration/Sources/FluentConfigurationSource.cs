using Donakunn.MessagingOverQueue.Configuration.Builders;
using Donakunn.MessagingOverQueue.Configuration.Options;

namespace Donakunn.MessagingOverQueue.Configuration.Sources;

/// <summary>
/// Configuration source that uses the fluent builder pattern.
/// This maintains backward compatibility with the existing API.
/// </summary>
public class FluentConfigurationSource : IRabbitMqConfigurationSource
{
    private readonly Action<RabbitMqOptionsBuilder> _configure;

    /// <summary>
    /// Default priority for fluent configuration (highest).
    /// </summary>
    public int Priority { get; }

    /// <summary>
    /// Creates a new fluent configuration source.
    /// </summary>
    /// <param name="configure">The configuration action.</param>
    /// <param name="priority">The priority (default: 100 for highest).</param>
    public FluentConfigurationSource(Action<RabbitMqOptionsBuilder> configure, int priority = 100)
    {
        _configure = configure ?? throw new ArgumentNullException(nameof(configure));
        Priority = priority;
    }

    /// <summary>
    /// Applies the fluent configuration.
    /// </summary>
    public void Configure(RabbitMqOptions options)
    {
        var builder = new RabbitMqOptionsBuilder();
        _configure(builder);
        var builtOptions = builder.Build();

        // Apply all configurations from the builder
        CopyOptions(builtOptions, options);
    }

    private static void CopyOptions(RabbitMqOptions source, RabbitMqOptions target)
    {
        target.HostName = source.HostName;
        target.Port = source.Port;
        target.UserName = source.UserName;
        target.Password = source.Password;
        target.VirtualHost = source.VirtualHost;
        target.ConnectionTimeout = source.ConnectionTimeout;
        target.RequestedHeartbeat = source.RequestedHeartbeat;
        target.UseSsl = source.UseSsl;
        target.SslServerName = source.SslServerName;
        target.ClientProvidedName = source.ClientProvidedName;
        target.ChannelPoolSize = source.ChannelPoolSize;
        target.AutomaticRecoveryEnabled = source.AutomaticRecoveryEnabled;
        target.NetworkRecoveryInterval = source.NetworkRecoveryInterval;

        // Merge collections (fluent takes precedence)
        target.Exchanges = source.Exchanges.Concat(target.Exchanges).ToList();
        target.Queues = source.Queues.Concat(target.Queues).ToList();
        target.Bindings = source.Bindings.Concat(target.Bindings).ToList();
    }
}
