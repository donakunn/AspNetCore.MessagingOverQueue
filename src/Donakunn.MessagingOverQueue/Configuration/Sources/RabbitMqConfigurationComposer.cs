using MessagingOverQueue.src.Configuration.Options;

namespace MessagingOverQueue.src.Configuration.Sources;

/// <summary>
/// Composes multiple configuration sources into a single configuration.
/// Sources are applied in order of priority (lowest to highest).
/// </summary>
public class RabbitMqConfigurationComposer
{
    private readonly List<IRabbitMqConfigurationSource> _sources = new();

    /// <summary>
    /// Adds a configuration source.
    /// </summary>
    public RabbitMqConfigurationComposer AddSource(IRabbitMqConfigurationSource source)
    {
        if (source == null)
            throw new ArgumentNullException(nameof(source));

        _sources.Add(source);
        return this;
    }

    /// <summary>
    /// Adds multiple configuration sources.
    /// </summary>
    public RabbitMqConfigurationComposer AddSources(IEnumerable<IRabbitMqConfigurationSource> sources)
    {
        if (sources == null)
            throw new ArgumentNullException(nameof(sources));

        _sources.AddRange(sources);
        return this;
    }

    /// <summary>
    /// Composes all sources into a final configuration.
    /// Sources are applied in order of priority (lowest to highest).
    /// </summary>
    public RabbitMqOptions Compose()
    {
        var options = new RabbitMqOptions();

        // Apply sources in order of priority (lowest to highest)
        var orderedSources = _sources.OrderBy(s => s.Priority);

        foreach (var source in orderedSources)
        {
            source.Configure(options);
        }

        return options;
    }

    /// <summary>
    /// Creates a configuration action for IServiceCollection.
    /// </summary>
    public Action<RabbitMqOptions> CreateConfigureAction()
    {
        return options =>
        {
            var composedOptions = Compose();
            CopyOptions(composedOptions, options);
        };
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
        target.Exchanges = source.Exchanges;
        target.Queues = source.Queues;
        target.Bindings = source.Bindings;
    }
}
