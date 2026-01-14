using Donakunn.MessagingOverQueue.Persistence.Providers;
using Donakunn.MessagingOverQueue.Persistence.Providers.SqlServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Donakunn.MessagingOverQueue.Persistence.DependencyInjection;

/// <summary>
/// Extension methods for registering message store providers.
/// </summary>
public static class MessageStoreProviderExtensions
{
    /// <summary>
    /// Adds the SQL Server message store provider.
    /// </summary>
    /// <param name="builder">The outbox builder.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <param name="configure">Optional configuration for message store options.</param>
    /// <returns>The outbox builder for chaining.</returns>
    public static IOutboxBuilder UseSqlServer(
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

        return builder;
    }

    /// <summary>
    /// Adds the SQL Server message store provider with configuration from a callback.
    /// </summary>
    /// <param name="builder">The outbox builder.</param>
    /// <param name="configure">Configuration callback for message store options.</param>
    /// <returns>The outbox builder for chaining.</returns>
    public static IOutboxBuilder UseSqlServer(
        this IOutboxBuilder builder,
        Action<MessageStoreOptions> configure)
    {
        builder.Services.Configure(configure);
        builder.Services.TryAddSingleton<IMessageStoreProvider, SqlServerMessageStoreProvider>();

        return builder;
    }
}

/// <summary>
/// Builder interface for configuring the outbox pattern.
/// </summary>
public interface IOutboxBuilder
{
    /// <summary>
    /// The service collection.
    /// </summary>
    IServiceCollection Services { get; }
}

/// <summary>
/// Default implementation of <see cref="IOutboxBuilder"/>.
/// </summary>
internal sealed class OutboxBuilder : IOutboxBuilder
{
    public IServiceCollection Services { get; }

    public OutboxBuilder(IServiceCollection services)
    {
        Services = services;
    }
}
