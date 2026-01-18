using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.Persistence;
using Donakunn.MessagingOverQueue.Persistence.Repositories;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

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
        Services.Configure<OutboxOptions>(options =>
        {
            configure?.Invoke(options);
        });

        // Register repositories (they delegate to the provider)
        Services.TryAddSingleton<IOutboxRepository, OutboxRepository>();
        Services.TryAddSingleton<IInboxRepository, InboxRepository>();

        // Register outbox publisher for transactional scenarios
        Services.AddScoped<OutboxPublisher>();

        // Register outbox processor background service
        Services.AddHostedService<OutboxProcessor>();

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
