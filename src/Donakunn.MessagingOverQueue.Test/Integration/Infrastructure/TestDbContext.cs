using Donakunn.MessagingOverQueue.Persistence;
using Donakunn.MessagingOverQueue.Persistence.Entities;
using Microsoft.EntityFrameworkCore;

namespace MessagingOverQueue.Test.Integration.Infrastructure;

/// <summary>
/// Test DbContext that implements IOutboxDbContext for outbox pattern testing.
/// </summary>
public class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options), IOutboxDbContext
{
    /// <summary>
    /// Outbox messages for reliable message delivery.
    /// </summary>
    public DbSet<OutboxMessage> OutboxMessages { get; set; } = null!;

    /// <summary>
    /// Inbox messages for idempotency.
    /// </summary>
    public DbSet<InboxMessage> InboxMessages { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.ConfigureOutbox();
    }
}
