using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.Persistence;
using Donakunn.MessagingOverQueue.Persistence.Providers;
using Donakunn.MessagingOverQueue.Persistence.Repositories;
using Donakunn.MessagingOverQueue.Providers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Donakunn.MessagingOverQueue.DependencyInjection.Persistence;

/// <summary>
/// Implementation of the persistence builder.
/// </summary>
internal sealed class PersistenceBuilder : IPersistenceBuilder
{
    public IServiceCollection Services { get; }
    public IMessagingBuilder MessagingBuilder { get; }

    public PersistenceBuilder(IServiceCollection services, IMessagingBuilder messagingBuilder)
    {
        Services = services;
        MessagingBuilder = messagingBuilder;
    }

    public IOutboxBuilder WithOutbox(Action<OutboxOptions>? configure = null)
    {
        // Build options to determine worker count
        var options = new OutboxOptions();
        configure?.Invoke(options);

        Services.Configure<OutboxOptions>(opts =>
        {
            configure?.Invoke(opts);
        });

        // Register repositories (they delegate to the provider)
        Services.TryAddSingleton<IOutboxRepository, OutboxRepository>();
        Services.TryAddSingleton<IInboxRepository, InboxRepository>();

        // Register outbox publisher for transactional scenarios
        Services.AddScoped<OutboxPublisher>();

        // Register outbox processor workers
        for (int i = 0; i < options.WorkerCount; i++)
        {
            var workerId = i; // Capture for lambda
            Services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService>(sp =>
            {
                return new OutboxProcessor(
                    sp.GetRequiredService<IOutboxRepository>(),
                    sp.GetRequiredService<IInboxRepository>(),
                    sp.GetRequiredService<IMessageStoreProvider>(),
                    sp.GetRequiredService<IInternalPublisher>(),
                    sp.GetRequiredService<IOptions<OutboxOptions>>(),
                    sp.GetRequiredService<ILogger<OutboxProcessor>>(),
                    workerId);
            });
        }

        return new OutboxBuilder(Services, this);
    }

    public IPersistenceBuilder WithIdempotency(Action<IdempotencyOptions>? configure = null)
    {
        Services.Configure<IdempotencyOptions>(options =>
        {
            configure?.Invoke(options);
        });

        // Register idempotency middleware (decoupled from outbox)
        Services.AddScoped<IdempotencyMiddleware>();
        Services.AddScoped<IConsumeMiddleware>(sp => sp.GetRequiredService<IdempotencyMiddleware>());

        return this;
    }
}

/// <summary>
/// Implementation of the outbox builder.
/// </summary>
internal sealed class OutboxBuilder : IOutboxBuilder
{
    public IServiceCollection Services { get; }
    public IPersistenceBuilder PersistenceBuilder { get; }

    public OutboxBuilder(IServiceCollection services, IPersistenceBuilder persistenceBuilder)
    {
        Services = services;
        PersistenceBuilder = persistenceBuilder;
    }
}
