using Donakunn.MessagingOverQueue.DependencyInjection;
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
/// Integration tests for the idempotency pattern using inbox messages.
/// Tests that duplicate messages are not processed multiple times.
/// </summary>
public class IdempotencyIntegrationTests : RabbitMqIntegrationTestBase
{
    [Fact]
    public async Task InboxRepository_Marks_Message_As_Processed()
    {
        // Arrange
        using var host = await BuildHostWithIdempotency();
        var testEvent = new SimpleTestEvent { Value = "IdempotencyTest" };
        var handlerType = typeof(SimpleTestEventHandler).FullName!;

        // Act - mark message as processed
        using (var scope = host.Services.CreateScope())
        {
            var inboxRepository = scope.ServiceProvider.GetRequiredService<IInboxRepository>();
            await inboxRepository.MarkAsProcessedAsync(testEvent, handlerType);
        }

        // Assert - verify message is in inbox via provider
        using (var scope = host.Services.CreateScope())
        {
            var provider = scope.ServiceProvider.GetRequiredService<IMessageStoreProvider>();
            var exists = await provider.ExistsInboxEntryAsync(testEvent.Id, handlerType);

            Assert.True(exists);
        }
    }

    [Fact]
    public async Task InboxRepository_HasBeenProcessed_Returns_True_For_Processed_Message()
    {
        // Arrange
        using var host = await BuildHostWithIdempotency();
        var testEvent = new SimpleTestEvent { Value = "ProcessedCheck" };
        var handlerType = typeof(SimpleTestEventHandler).FullName!;

        // First mark as processed
        using (var scope = host.Services.CreateScope())
        {
            var inboxRepository = scope.ServiceProvider.GetRequiredService<IInboxRepository>();
            await inboxRepository.MarkAsProcessedAsync(testEvent, handlerType);
        }

        // Act & Assert
        using (var scope = host.Services.CreateScope())
        {
            var inboxRepository = scope.ServiceProvider.GetRequiredService<IInboxRepository>();
            var hasBeenProcessed = await inboxRepository.HasBeenProcessedAsync(testEvent.Id, handlerType);

            Assert.True(hasBeenProcessed);
        }
    }

    [Fact]
    public async Task InboxRepository_HasBeenProcessed_Returns_False_For_New_Message()
    {
        // Arrange
        using var host = await BuildHostWithIdempotency();
        var newMessageId = Guid.NewGuid();
        var handlerType = typeof(SimpleTestEventHandler).FullName!;

        // Act & Assert
        using (var scope = host.Services.CreateScope())
        {
            var inboxRepository = scope.ServiceProvider.GetRequiredService<IInboxRepository>();
            var hasBeenProcessed = await inboxRepository.HasBeenProcessedAsync(newMessageId, handlerType);

            Assert.False(hasBeenProcessed);
        }
    }

    [Fact]
    public async Task Idempotency_Middleware_Processes_Message_First_Time()
    {
        // Arrange
        IdempotentTestEventHandler.Reset();
        using var host = await BuildHostWithIdempotency();
        var testEvent = new IdempotentTestEvent { Value = "FirstTimeProcessing" };

        // Act - publish message
        using (var scope = host.Services.CreateScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<Donakunn.MessagingOverQueue.Abstractions.Publishing.IEventPublisher>();
            await publisher.PublishAsync(testEvent);
        }

        // Wait for message to be processed
        await IdempotentTestEventHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(15));

        // Assert - handler should have been called
        Assert.Equal(1, IdempotentTestEventHandler.HandleCount);
        Assert.Contains(IdempotentTestEventHandler.HandledMessages, m => m.Value == "FirstTimeProcessing");
    }

    [Fact]
    public async Task Idempotency_Middleware_Records_Processed_Message_In_Inbox()
    {
        // Arrange
        IdempotentTestEventHandler.Reset();
        using var host = await BuildHostWithIdempotency();
        var testEvent = new IdempotentTestEvent { Value = "RecordInInbox" };

        // Act - publish message
        using (var scope = host.Services.CreateScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<Donakunn.MessagingOverQueue.Abstractions.Publishing.IEventPublisher>();
            await publisher.PublishAsync(testEvent);
        }

        // Wait for message to be processed
        await IdempotentTestEventHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(15));

        // Assert - message should be recorded in inbox
        using (var scope = host.Services.CreateScope())
        {
            var provider = scope.ServiceProvider.GetRequiredService<IMessageStoreProvider>();
            var handlerType = typeof(IdempotentTestEventHandler).FullName!;
            var exists = await provider.ExistsInboxEntryAsync(testEvent.Id, handlerType);

            Assert.True(exists);
        }
    }

    [Fact]
    public async Task Republishing_Same_Message_Is_Idempotent()
    {
        // Arrange
        IdempotentTestEventHandler.Reset();
        using var host = await BuildHostWithIdempotency();
        var testEvent = new IdempotentTestEvent { Value = "RepublishTest" };

        // Act - publish the same message multiple times
        using (var scope = host.Services.CreateScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<Donakunn.MessagingOverQueue.Abstractions.Publishing.IEventPublisher>();

            await publisher.PublishAsync(testEvent);
            await publisher.PublishAsync(testEvent);
            await publisher.PublishAsync(testEvent);
        }

        // Wait for first message to be processed
        await IdempotentTestEventHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(15));

        // Give some time for any additional messages to be processed
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Assert - handler should only have been called once
        Assert.Equal(1, IdempotentTestEventHandler.HandleCount);
    }

    private async Task<IHost> BuildHostWithIdempotency()
    {
        return await BuildHost(services =>
        {
            services.AddRabbitMqMessaging(options =>
            {
                options.UseHost(_rabbitMqContainer!.Hostname);
                options.UsePort(_rabbitMqContainer!.GetMappedPublicPort(5672));
                options.WithCredentials("guest", "guest");
                options.WithConnectionName($"IdempotencyTests-{Guid.NewGuid():N}");
            })
            .AddTopology(topology => topology
                .WithServiceName("idempotency-test-service")
                .ScanAssemblyContaining<IdempotentTestEventHandler>())
            .AddOutboxPattern(options =>
            {
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
