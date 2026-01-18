using Donakunn.MessagingOverQueue.DependencyInjection;
using Microsoft.Extensions.Configuration;

namespace Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection.Queues;

/// <summary>
/// Extension methods for configuring Redis Streams as the queue provider.
/// </summary>
public static class RedisStreamsQueueExtensions
{
    /// <summary>
    /// Configures Redis Streams as the queue provider.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="configure">Configuration action for the Redis Streams queue builder.</param>
    /// <returns>The messaging builder for chaining.</returns>
    /// <example>
    /// <code>
    /// services.AddMessaging()
    ///     .UseRedisStreamsQueues(queues => queues
    ///         .WithConnection(opts => opts.UseConnectionString("localhost:6379"))
    ///         .WithTopology(t => t.ScanAssemblyContaining&lt;OrderHandler&gt;())
    ///         .WithHealthChecks());
    /// </code>
    /// </example>
    public static IMessagingBuilder UseRedisStreamsQueues(
        this IMessagingBuilder builder,
        Action<IRedisStreamsQueueBuilder> configure)
    {
        var queueBuilder = new RedisStreamsQueueBuilder(builder.Services, builder);
        configure(queueBuilder);

        // Ensure core services are registered even if no configuration methods were called
        queueBuilder.EnsureCoreServicesRegistered();

        return builder;
    }

    /// <summary>
    /// Configures Redis Streams as the queue provider using IConfiguration.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="configuration">The configuration instance.</param>
    /// <param name="configure">Optional additional configuration.</param>
    /// <param name="sectionName">The configuration section name (default: "RedisStreams").</param>
    /// <returns>The messaging builder for chaining.</returns>
    public static IMessagingBuilder UseRedisStreamsQueues(
        this IMessagingBuilder builder,
        IConfiguration configuration,
        Action<IRedisStreamsQueueBuilder>? configure = null,
        string? sectionName = null)
    {
        return builder.UseRedisStreamsQueues(queues =>
        {
            // Load from configuration
            queues.WithConnection(opts =>
            {
                var section = configuration.GetSection(sectionName ?? "RedisStreams");
                if (section.Exists())
                {
                    var connectionString = section["ConnectionString"];
                    if (!string.IsNullOrEmpty(connectionString))
                        opts.UseConnectionString(connectionString);

                    var password = section["Password"];
                    if (!string.IsNullOrEmpty(password))
                        opts.WithPassword(password);

                    var databaseStr = section["Database"];
                    if (int.TryParse(databaseStr, out var database))
                        opts.UseDatabase(database);

                    var streamPrefix = section["StreamPrefix"];
                    if (!string.IsNullOrEmpty(streamPrefix))
                        opts.WithStreamPrefix(streamPrefix);
                }
            });

            // Apply additional configuration
            configure?.Invoke(queues);
        });
    }
}
