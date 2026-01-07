using MessagingOverQueue.src.Persistence;
using Microsoft.EntityFrameworkCore;

namespace MessagingOverQueue.Test.Integration.Infrastructure;

public class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.ConfigureOutbox();
    }
}
