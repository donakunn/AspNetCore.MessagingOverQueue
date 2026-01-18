using Donakunn.MessagingOverQueue.Persistence.Providers;
using Donakunn.MessagingOverQueue.Persistence.Providers.SqlServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Donakunn.MessagingOverQueue.DependencyInjection.Persistence;

/// <summary>
/// Extension methods for configuring persistence patterns.
/// </summary>
public static class PersistenceExtensions
{
    /// <summary>
    /// Configures persistence patterns (outbox/inbox, idempotency) for reliable messaging.
    /// </summary>
    /// <param name="builder">The messaging builder.</param>
    /// <param name="configure">Configuration action for the persistence builder.</param>
    /// <returns>The messaging builder for chaining.</returns>
    /// <example>
    /// <code>
    /// services.AddMessaging()
    ///     .UseRabbitMqQueues(...)
    ///     .UsePersistence(persistence => persistence
    ///         .WithOutbox(opts => opts.BatchSize = 50)
    ///             .UseSqlServer(connectionString)
    ///         .WithIdempotency());
    /// </code>
    /// </example>
    public static IMessagingBuilder UsePersistence(
        this IMessagingBuilder builder,
        Action<IPersistenceBuilder> configure)
    {
        var persistenceBuilder = new PersistenceBuilder(builder.Services, builder);
        configure(persistenceBuilder);
        return builder;
    }

    /// <summary>
    /// Configures SQL Server as the outbox database provider.
    /// </summary>
    /// <param name="builder">The outbox builder.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <param name="configure">Optional configuration for message store options.</param>
    /// <returns>The persistence builder for chaining.</returns>
    public static IPersistenceBuilder UseSqlServer(
        this IOutboxBuilder builder,
        string connectionString,
        Action<MessageStoreOptions>? configure = null)
    {
        builder.Services.Configure<MessageStoreOptions>(options =>
        {
            options.ConnectionString = connectionString;
            configure?.Invoke(options);
        });

        builder.Services.TryAddSingleton<IMessageStoreProvider, SqlServerMessageStoreProvider>();

        return builder.PersistenceBuilder;
    }

    /// <summary>
    /// Configures SQL Server as the outbox database provider with configuration callback.
    /// </summary>
    /// <param name="builder">The outbox builder.</param>
    /// <param name="configure">Configuration callback for message store options.</param>
    /// <returns>The persistence builder for chaining.</returns>
    public static IPersistenceBuilder UseSqlServer(
        this IOutboxBuilder builder,
        Action<MessageStoreOptions> configure)
    {
        builder.Services.Configure(configure);
        builder.Services.TryAddSingleton<IMessageStoreProvider, SqlServerMessageStoreProvider>();

        return builder.PersistenceBuilder;
    }
}
