using Microsoft.Extensions.Configuration;

namespace Donakunn.MessagingOverQueue.DependencyInjection.Queues;

/// <summary>
/// Extension methods for configuring RabbitMQ as the queue provider.
/// </summary>
public static class RabbitMqQueueExtensions
{
    /// <summary>
    /// Configures RabbitMQ as the queue provider.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="configure">Configuration action for the RabbitMQ queue builder.</param>
    /// <returns>The messaging builder for chaining.</returns>
    /// <example>
    /// <code>
    /// services.AddMessaging()
    ///     .UseRabbitMqQueues(queues => queues
    ///         .WithConnection(opts => opts.UseHost("localhost"))
    ///         .WithTopology(t => t.ScanAssemblyContaining&lt;OrderHandler&gt;())
    ///         .WithHealthChecks());
    /// </code>
    /// </example>
    public static IMessagingBuilder UseRabbitMqQueues(
        this IMessagingBuilder builder,
        Action<IRabbitMqQueueBuilder> configure)
    {
        var queueBuilder = new RabbitMqQueueBuilder(builder.Services, builder);
        configure(queueBuilder);

        // Ensure core services are registered even if no configuration methods were called
        queueBuilder.EnsureCoreServicesRegistered();

        return builder;
    }

    /// <summary>
    /// Configures RabbitMQ as the queue provider using IConfiguration.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="configuration">The configuration instance.</param>
    /// <param name="configure">Optional additional configuration.</param>
    /// <param name="sectionName">The configuration section name (default: "RabbitMq").</param>
    /// <returns>The messaging builder for chaining.</returns>
    public static IMessagingBuilder UseRabbitMqQueues(
        this IMessagingBuilder builder,
        IConfiguration configuration,
        Action<IRabbitMqQueueBuilder>? configure = null,
        string? sectionName = null)
    {
        return builder.UseRabbitMqQueues(queues =>
        {
            // Load from configuration
            queues.WithConnection(opts =>
            {
                var section = configuration.GetSection(sectionName ?? "RabbitMq");
                if (section.Exists())
                {
                    var hostName = section["HostName"];
                    if (!string.IsNullOrEmpty(hostName))
                        opts.UseHost(hostName);

                    var portStr = section["Port"];
                    if (int.TryParse(portStr, out var port))
                        opts.UsePort(port);

                    var userName = section["UserName"];
                    var password = section["Password"];
                    if (!string.IsNullOrEmpty(userName))
                        opts.WithCredentials(userName, password ?? "");

                    var virtualHost = section["VirtualHost"];
                    if (!string.IsNullOrEmpty(virtualHost))
                        opts.UseVirtualHost(virtualHost);
                }
            });

            // Apply additional configuration
            configure?.Invoke(queues);
        });
    }
}
