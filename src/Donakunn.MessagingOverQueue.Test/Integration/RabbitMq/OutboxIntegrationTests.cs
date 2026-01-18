using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.DependencyInjection;
using Donakunn.MessagingOverQueue.Persistence;
using Donakunn.MessagingOverQueue.Persistence.DependencyInjection;
using Donakunn.MessagingOverQueue.Persistence.Entities;
using Donakunn.MessagingOverQueue.Persistence.Providers;
using Donakunn.MessagingOverQueue.Persistence.Repositories;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;
using MessagingOverQueue.Test.Integration.RabbitMq.Infrastructure;
using MessagingOverQueue.Test.Integration.Shared.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MessagingOverQueue.Test.Integration.RabbitMq;

/// <summary>
/// Integration tests for the outbox pattern.
/// Tests reliable message delivery using SQL Server provider.
/// </summary>
public class OutboxIntegrationTests : RabbitMqIntegrationTestBase
{
    [Fact]
    public async Task OutboxPublisher_Stores_Message_In_Database()
    {
        // Arrange
        using var host = await BuildHostWithOutbox();
        var testEvent = new SimpleTestEvent { Value = "OutboxTest" };

        // Act - use scoped services
        using (var scope = host.Services.CreateScope())
        {
            var outboxPublisher = scope.ServiceProvider.GetRequiredService<OutboxPublisher>();
            await ((IEventPublisher)outboxPublisher).PublishAsync(testEvent);
        }

        // Assert - verify message was stored
        using (var scope = host.Services.CreateScope())
        {
            var provider = scope.ServiceProvider.GetRequiredService<IMessageStoreProvider>();
            var entry = await provider.GetByIdAsync(testEvent.Id, MessageDirection.Outbox);

            Assert.NotNull(entry);
            Assert.Equal(MessageStatus.Pending, entry.Status);
            Assert.NotNull(entry.Payload);
            Assert.True(entry.Payload.Length > 0);
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
            await ((IEventPublisher)outboxPublisher).PublishAsync(testEvent);
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

        // Act - store and let processor run
        using (var scope = host.Services.CreateScope())
        {
            var outboxPublisher = scope.ServiceProvider.GetRequiredService<OutboxPublisher>();
            await ((IEventPublisher)outboxPublisher).PublishAsync(testEvent);
        }

        // Wait for handler to process
        await SimpleTestEventHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(15));

        // Allow time for status update
        await Task.Delay(500);

        // Assert - verify status changed to Published
        using (var scope = host.Services.CreateScope())
        {
            var provider = scope.ServiceProvider.GetRequiredService<IMessageStoreProvider>();
            var entry = await provider.GetByIdAsync(testEvent.Id, MessageDirection.Outbox);

            Assert.NotNull(entry);
            Assert.Equal(MessageStatus.Published, entry.Status);
            Assert.NotNull(entry.ProcessedAt);
        }
    }

    [Fact]
    public async Task OutboxRepository_AcquireLock_Sets_Processing_Status()
    {
        // Arrange
        using var host = await BuildHostWithOutbox();
        var testEvent = new SimpleTestEvent { Value = "LockTest" };

        // Store message
        using (var scope = host.Services.CreateScope())
        {
            var outboxPublisher = scope.ServiceProvider.GetRequiredService<OutboxPublisher>();
            await ((IEventPublisher)outboxPublisher).PublishAsync(testEvent);
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
            Assert.Equal(MessageStatus.Processing, lockedMessage.Status);
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

        // Create an old published message directly via provider
        using (var scope = host.Services.CreateScope())
        {
            var provider = scope.ServiceProvider.GetRequiredService<IMessageStoreProvider>();
            var oldEntry = new MessageStoreEntry
            {
                Id = oldMessageId,
                Direction = MessageDirection.Outbox,
                MessageType = "TestType",
                Payload = new byte[] { 1, 2, 3 },
                CreatedAt = DateTime.UtcNow.AddDays(-10),
                ProcessedAt = DateTime.UtcNow.AddDays(-10),
                Status = MessageStatus.Published
            };
            await provider.AddAsync(oldEntry);
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
            var provider = scope.ServiceProvider.GetRequiredService<IMessageStoreProvider>();
            var entry = await provider.GetByIdAsync(oldMessageId, MessageDirection.Outbox);
            Assert.Null(entry);
        }
    }

    private async Task<IHost> BuildHostWithOutbox(
        TimeSpan? processingInterval = null,
        int batchSize = 100)
    {
        return await BuildHost(services =>
        {
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
            .AddOutboxPattern(options =>
            {
                options.ProcessingInterval = processingInterval ?? TimeSpan.FromSeconds(5);
                options.BatchSize = batchSize;
                options.Enabled = true;
                options.AutoCleanup = true;
                options.RetentionPeriod = TimeSpan.FromHours(1);
                options.AutoCreateSchema = true;
            })
            .UseSqlServer(ConnectionString, store =>
            {
                store.TableName = "MessageStore";
                store.AutoCreateSchema = true;
            });
        });
    }

    protected override void ConfigureAdditionalServices(IServiceCollection services)
    {
        // No additional services needed - SQL Server provider is configured per test
    }
}
