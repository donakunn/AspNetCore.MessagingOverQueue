using Donakunn.MessagingOverQueue.Configuration.Options;
using Microsoft.Extensions.DependencyInjection;
using OldOutboxBuilder = Donakunn.MessagingOverQueue.Persistence.DependencyInjection.IOutboxBuilder;

namespace Donakunn.MessagingOverQueue.DependencyInjection.Persistence;

/// <summary>
/// Builder interface for configuring persistence patterns (outbox/inbox, idempotency).
/// </summary>
public interface IPersistenceBuilder
{
    /// <summary>
    /// The service collection.
    /// </summary>
    IServiceCollection Services { get; }

    /// <summary>
    /// The parent messaging builder.
    /// </summary>
    IMessagingBuilder MessagingBuilder { get; }

    /// <summary>
    /// Configures the outbox pattern for reliable messaging.
    /// </summary>
    /// <param name="configure">Configuration action for outbox options.</param>
    /// <returns>The outbox builder for configuring the database provider.</returns>
    IOutboxBuilder WithOutbox(Action<OutboxOptions>? configure = null);

    /// <summary>
    /// Configures idempotency for message processing.
    /// This can be used independently of the outbox pattern.
    /// </summary>
    /// <param name="configure">Configuration action for idempotency options.</param>
    /// <returns>The builder for chaining.</returns>
    IPersistenceBuilder WithIdempotency(Action<IdempotencyOptions>? configure = null);
}

/// <summary>
/// Builder interface for configuring the outbox pattern database provider.
/// Extends the legacy IOutboxBuilder for backward compatibility with existing UseSqlServer extensions.
/// </summary>
public interface IOutboxBuilder : OldOutboxBuilder
{
    /// <summary>
    /// The parent persistence builder for fluent chaining back to persistence configuration.
    /// </summary>
    IPersistenceBuilder PersistenceBuilder { get; }
}

/// <summary>
/// Configuration options for idempotency.
/// </summary>
public class IdempotencyOptions
{
    /// <summary>
    /// Whether idempotency is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Retention period for processed message IDs.
    /// </summary>
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Message types to exclude from idempotency checks.
    /// </summary>
    public List<string> ExcludedMessageTypes { get; set; } = [];
}
