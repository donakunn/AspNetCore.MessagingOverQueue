using MessagingOverQueue.src.Persistence.Entities;
using Microsoft.EntityFrameworkCore;

namespace MessagingOverQueue.src.Persistence;

/// <summary>
/// Interface for DbContext that supports the outbox pattern.
/// User's DbContext should implement this interface.
/// </summary>
public interface IOutboxDbContext
{
    /// <summary>
    /// Outbox messages.
    /// </summary>
    DbSet<OutboxMessage> OutboxMessages { get; set; }

    /// <summary>
    /// Inbox messages for idempotency.
    /// </summary>
    DbSet<InboxMessage> InboxMessages { get; set; }

    /// <summary>
    /// Saves changes to the database.
    /// </summary>
    Task<int> SaveChangesAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Extension methods for configuring the outbox model.
/// </summary>
public static class OutboxModelBuilderExtensions
{
    /// <summary>
    /// Configures the outbox and inbox entities in the model.
    /// Call this from your DbContext's OnModelCreating method.
    /// </summary>
    public static ModelBuilder ConfigureOutbox(this ModelBuilder modelBuilder, string? schema = null)
    {
        modelBuilder.Entity<OutboxMessage>(entity =>
        {
            if (schema != null)
                entity.ToTable("OutboxMessages", schema);
            else
                entity.ToTable("OutboxMessages");

            entity.HasKey(e => e.Id);
            entity.Property(e => e.MessageType).IsRequired().HasMaxLength(500);
            entity.Property(e => e.Payload).IsRequired();
            entity.Property(e => e.ExchangeName).HasMaxLength(256);
            entity.Property(e => e.RoutingKey).HasMaxLength(256);
            entity.Property(e => e.LockToken).HasMaxLength(100);
            entity.Property(e => e.CorrelationId).HasMaxLength(100);
            entity.Property(e => e.LastError).HasMaxLength(4000);

            entity.HasIndex(e => new { e.Status, e.CreatedAt });
            entity.HasIndex(e => e.LockExpiresAt);
            entity.HasIndex(e => e.CorrelationId);
        });

        modelBuilder.Entity<InboxMessage>(entity =>
        {
            if (schema != null)
                entity.ToTable("InboxMessages", schema);
            else
                entity.ToTable("InboxMessages");

            entity.HasKey(e => new { e.Id, e.HandlerType });
            entity.Property(e => e.MessageType).IsRequired().HasMaxLength(500);
            entity.Property(e => e.HandlerType).IsRequired().HasMaxLength(500);
            entity.Property(e => e.CorrelationId).HasMaxLength(100);

            entity.HasIndex(e => e.ProcessedAt);
            entity.HasIndex(e => e.CorrelationId);
        });

        return modelBuilder;
    }
}

