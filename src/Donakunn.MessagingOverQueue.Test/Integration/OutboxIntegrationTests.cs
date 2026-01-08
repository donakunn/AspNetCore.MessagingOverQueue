using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.DependencyInjection;
using Donakunn.MessagingOverQueue.Persistence;
using Donakunn.MessagingOverQueue.Persistence.Entities;
using Donakunn.MessagingOverQueue.Persistence.Repositories;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;
using MessagingOverQueue.Test.Integration.Infrastructure;
using MessagingOverQueue.Test.Integration.TestDoubles;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using static Donakunn.MessagingOverQueue.Topology.DependencyInjection.TopologyServiceCollectionExtensions;

namespace MessagingOverQueue.Test.Integration;

/// <summary>
/// Integration tests for the outbox pattern.
/// Tests reliable message delivery using database transactions.
/// </summary>
public class OutboxIntegrationTests : IntegrationTestBase
{
    [Fact]
    public async Task OutboxPublisher_Stores_Message_In_Database()
    {
        // Arrange
        using var host = await BuildHostWithOutbox();
        var testEvent = new SimpleTestEvent { Value = "OutboxTest" };

        // Act - use scoped services to avoid DbContext threading issues
        using (var scope = host.Services.CreateScope())
        {
            var outboxPublisher = scope.ServiceProvider.GetRequiredService<OutboxPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            await ((IEventPublisher)outboxPublisher).PublishAsync(testEvent);
            await dbContext.SaveChangesAsync();
        }

        // Assert - use a new scope to verify
        using (var scope = host.Services.CreateScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var outboxMessage = await dbContext.OutboxMessages
                .FirstOrDefaultAsync(m => m.Id == testEvent.Id);

            Assert.NotNull(outboxMessage);
            Assert.Equal(OutboxMessageStatus.Pending, outboxMessage.Status);
            Assert.NotNull(outboxMessage.Payload);
            Assert.True(outboxMessage.Payload.Length > 0);
        }
    }

    [Fact]
    public async Task OutboxProcessor_Publishes_Pending_Messages_To_RabbitMq()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithOutbox(processingInterval: TimeSpan.FromSeconds(1));
        var testEvent = new SimpleTestEvent { Value = "ProcessorTest" };

        // Act - store message in outbox
        using (var scope = host.Services.CreateScope())
        {
            var outboxPublisher = scope.ServiceProvider.GetRequiredService<OutboxPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            await ((IEventPublisher)outboxPublisher).PublishAsync(testEvent);
            await dbContext.SaveChangesAsync();
        }

        // Wait for the processor to pick up and publish the message
        await SimpleTestEventHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(15));

        // Assert
        Assert.Equal(1, SimpleTestEventHandler.HandleCount);
        var handledMessage = SimpleTestEventHandler.HandledMessages.First();
        Assert.Equal("ProcessorTest", handledMessage.Value);
    }

    [Fact]
    public async Task OutboxMessage_Is_Marked_As_Published_After_Processing()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithOutbox(processingInterval: TimeSpan.FromSeconds(1));
        var testEvent = new SimpleTestEvent { Value = "StatusTest" };

        // Act - store message
        using (var scope = host.Services.CreateScope())
        {
            var outboxPublisher = scope.ServiceProvider.GetRequiredService<OutboxPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            await ((IEventPublisher)outboxPublisher).PublishAsync(testEvent);
            await dbContext.SaveChangesAsync();
        }

        // Wait for processing
        await SimpleTestEventHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(15));

        // Give the processor time to update status
        await Task.Delay(500);

        // Assert - use fresh scope
        using (var scope = host.Services.CreateScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var outboxMessage = await dbContext.OutboxMessages
                .FirstOrDefaultAsync(m => m.Id == testEvent.Id);

            Assert.NotNull(outboxMessage);
            Assert.Equal(OutboxMessageStatus.Published, outboxMessage.Status);
            Assert.NotNull(outboxMessage.ProcessedAt);
        }
    }

    [Fact]
    public async Task OutboxPublisher_Stores_Command_With_Correct_RoutingKey()
    {
        // Arrange
        using var host = await BuildHostWithOutbox();
        var command = new SimpleTestCommand { Action = "TestAction" };

        // Act
        using (var scope = host.Services.CreateScope())
        {
            var outboxPublisher = scope.ServiceProvider.GetRequiredService<OutboxPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            await ((ICommandSender)outboxPublisher).SendAsync(command);
            await dbContext.SaveChangesAsync();
        }

        // Assert
        using (var scope = host.Services.CreateScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var outboxMessage = await dbContext.OutboxMessages
                .FirstOrDefaultAsync(m => m.Id == command.Id);

            Assert.NotNull(outboxMessage);
            Assert.NotNull(outboxMessage.RoutingKey);
            // The routing key follows the topology naming convention (kebab-case with suffix removed)
            // SimpleTestCommand -> simple.test (for routing key format)
            Assert.Contains("simple", outboxMessage.RoutingKey, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public async Task Multiple_Messages_In_Single_Transaction_Are_All_Stored()
    {
        // Arrange
        using var host = await BuildHostWithOutbox();
        var events = Enumerable.Range(0, 5)
            .Select(i => new SimpleTestEvent { Value = $"Batch-{i}" })
            .ToList();

        // Act - simulate a transaction
        using (var scope = host.Services.CreateScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var outboxPublisher = scope.ServiceProvider.GetRequiredService<OutboxPublisher>();

            await using var transaction = await dbContext.Database.BeginTransactionAsync();

            foreach (var evt in events)
            {
                await ((IEventPublisher)outboxPublisher).PublishAsync(evt);
            }

            await dbContext.SaveChangesAsync();
            await transaction.CommitAsync();
        }

        // Assert
        using (var scope = host.Services.CreateScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var storedMessages = await dbContext.OutboxMessages
                .Where(m => m.Status == OutboxMessageStatus.Pending)
                .ToListAsync();

            Assert.Equal(5, storedMessages.Count);
            foreach (var evt in events)
            {
                Assert.Contains(storedMessages, m => m.Id == evt.Id);
            }
        }
    }

    [Fact]
    public async Task Transaction_Rollback_Does_Not_Store_Outbox_Messages()
    {
        // Arrange
        using var host = await BuildHostWithOutbox();
        var testEvent = new SimpleTestEvent { Value = "RollbackTest" };
        var eventId = testEvent.Id;

        // Act - rollback transaction
        using (var scope = host.Services.CreateScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var outboxPublisher = scope.ServiceProvider.GetRequiredService<OutboxPublisher>();

            await using var transaction = await dbContext.Database.BeginTransactionAsync();

            await ((IEventPublisher)outboxPublisher).PublishAsync(testEvent);
            await dbContext.SaveChangesAsync();
            await transaction.RollbackAsync();
        }

        // Assert - message should not be in database
        using (var scope = host.Services.CreateScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var outboxMessage = await dbContext.OutboxMessages
                .FirstOrDefaultAsync(m => m.Id == eventId);

            Assert.Null(outboxMessage);
        }
    }

    [Fact]
    public async Task OutboxProcessor_Processes_Messages_In_Batches()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithOutbox(
            processingInterval: TimeSpan.FromSeconds(1),
            batchSize: 5);

        // Create more messages than batch size
        var events = Enumerable.Range(0, 10)
            .Select(i => new SimpleTestEvent { Value = $"Batch-{i}" })
            .ToList();

        // Act
        using (var scope = host.Services.CreateScope())
        {
            var outboxPublisher = scope.ServiceProvider.GetRequiredService<OutboxPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            foreach (var evt in events)
            {
                await ((IEventPublisher)outboxPublisher).PublishAsync(evt);
            }
            await dbContext.SaveChangesAsync();
        }

        // Wait for all messages to be processed (may take multiple batches)
        await SimpleTestEventHandler.WaitForCountAsync(10, TimeSpan.FromSeconds(30));

        // Assert
        Assert.Equal(10, SimpleTestEventHandler.HandleCount);
    }

    [Fact]
    public async Task OutboxMessage_Preserves_CorrelationId()
    {
        // Arrange
        using var host = await BuildHostWithOutbox();
        var correlationId = Guid.NewGuid().ToString();
        var testEvent = new SimpleTestEvent { Value = "CorrelationTest" };

        // Set correlation ID
        typeof(SimpleTestEvent).GetProperty("CorrelationId")?.SetValue(testEvent, correlationId);

        // Act
        using (var scope = host.Services.CreateScope())
        {
            var outboxPublisher = scope.ServiceProvider.GetRequiredService<OutboxPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            await ((IEventPublisher)outboxPublisher).PublishAsync(testEvent);
            await dbContext.SaveChangesAsync();
        }

        // Assert
        using (var scope = host.Services.CreateScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var outboxMessage = await dbContext.OutboxMessages
                .FirstOrDefaultAsync(m => m.Id == testEvent.Id);

            Assert.NotNull(outboxMessage);
            Assert.Equal(correlationId, outboxMessage.CorrelationId);
        }
    }

    [Fact]
    public async Task Outbox_And_Regular_Publisher_Can_Coexist()
    {
        // Arrange
        SimpleTestEventHandler.Reset();
        using var host = await BuildHostWithOutbox(processingInterval: TimeSpan.FromSeconds(1));

        // Act - publish through outbox
        var outboxEvent = new SimpleTestEvent { Value = "ViaOutbox" };
        using (var scope = host.Services.CreateScope())
        {
            var outboxPublisher = scope.ServiceProvider.GetRequiredService<OutboxPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            await ((IEventPublisher)outboxPublisher).PublishAsync(outboxEvent);
            await dbContext.SaveChangesAsync();
        }

        // Publish directly (using different scope)
        var directEvent = new SimpleTestEvent { Value = "ViaDirect" };
        using (var scope = host.Services.CreateScope())
        {
            var regularPublisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();
            await regularPublisher.PublishAsync(directEvent);
        }

        // Wait for both messages
        await SimpleTestEventHandler.WaitForCountAsync(2, TimeSpan.FromSeconds(15));

        // Assert
        Assert.Equal(2, SimpleTestEventHandler.HandleCount);
        Assert.Contains(SimpleTestEventHandler.HandledMessages, m => m.Value == "ViaOutbox");
        Assert.Contains(SimpleTestEventHandler.HandledMessages, m => m.Value == "ViaDirect");
    }

    [Fact]
    public async Task OutboxRepository_Can_Acquire_Lock_On_Pending_Messages()
    {
        // Arrange
        using var host = await BuildHostWithOutbox();
        var testEvent = new SimpleTestEvent { Value = "LockTest" };

        // Store a message
        using (var scope = host.Services.CreateScope())
        {
            var outboxPublisher = scope.ServiceProvider.GetRequiredService<OutboxPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            await ((IEventPublisher)outboxPublisher).PublishAsync(testEvent);
            await dbContext.SaveChangesAsync();
        }

        // Act - acquire lock in new scope
        using (var scope = host.Services.CreateScope())
        {
            var repository = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();
            var lockedMessages = await repository.AcquireLockAsync(10, TimeSpan.FromMinutes(5));

            // Assert
            Assert.Single(lockedMessages);
            var lockedMessage = lockedMessages.First();
            Assert.Equal(testEvent.Id, lockedMessage.Id);
            Assert.Equal(OutboxMessageStatus.Processing, lockedMessage.Status);
            Assert.NotNull(lockedMessage.LockToken);
            Assert.NotNull(lockedMessage.LockExpiresAt);
        }
    }

    [Fact]
    public async Task OutboxRepository_Cleanup_Removes_Old_Published_Messages()
    {
        // Arrange
        using var host = await BuildHostWithOutbox();
        var oldMessageId = Guid.NewGuid();

        // Create an old published message directly in database
        using (var scope = host.Services.CreateScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var oldMessage = new OutboxMessage
            {
                Id = oldMessageId,
                MessageType = "TestType",
                Payload = new byte[] { 1, 2, 3 },
                CreatedAt = DateTime.UtcNow.AddDays(-10),
                ProcessedAt = DateTime.UtcNow.AddDays(-10),
                Status = OutboxMessageStatus.Published
            };
            dbContext.OutboxMessages.Add(oldMessage);
            await dbContext.SaveChangesAsync();
        }

        // Act - cleanup with 1 day retention
        using (var scope = host.Services.CreateScope())
        {
            var repository = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();
            await repository.CleanupAsync(TimeSpan.FromDays(1));
        }

        // Assert
        using (var scope = host.Services.CreateScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var message = await dbContext.OutboxMessages.FindAsync(oldMessageId);
            Assert.Null(message);
        }
    }

    private async Task<IHost> BuildHostWithOutbox(
        TimeSpan? processingInterval = null,
        int batchSize = 100)
    {
        return await BuildHost(services =>
        {
            // Add DbContext with SQL Server (from base class's SQL container)
            services.AddDbContext<TestDbContext>(options =>
                options.UseSqlServer(ConnectionString));

            services.AddRabbitMqMessaging(options =>
            {
                options.UseHost(_rabbitMqContainer!.Hostname);
                options.UsePort(_rabbitMqContainer!.GetMappedPublicPort(5672));
                options.WithCredentials("guest", "guest");
                options.WithConnectionName($"OutboxTests-{Guid.NewGuid():N}");
            })
            .AddTopology(topology => topology
                .WithServiceName("outbox-test-service")
                .ScanAssemblyContaining<SimpleTestEventHandler>())
            .AddOutboxPattern<TestDbContext>(options =>
            {
                options.ProcessingInterval = processingInterval ?? TimeSpan.FromSeconds(5);
                options.BatchSize = batchSize;
                options.Enabled = true;
                options.AutoCleanup = true;
                options.RetentionPeriod = TimeSpan.FromHours(1);
            });
        });
    }

    protected override void ConfigureAdditionalServices(IServiceCollection services)
    {
        // No additional services needed - base class handles DbContext registration
    }
}
