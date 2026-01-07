using MessagingOverQueue.src.Abstractions.Publishing;
using MessagingOverQueue.src.DependencyInjection;
using MessagingOverQueue.Test.Integration.Infrastructure;
using MessagingOverQueue.Test.Integration.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using static MessagingOverQueue.src.Topology.DependencyInjection.TopologyServiceCollectionExtensions;

namespace MessagingOverQueue.Test.Integration;

/// <summary>
/// Integration tests for command sending and handling.
/// </summary>
public class CommandIntegrationTests : IntegrationTestBase
{
    [Fact]
    public async Task Command_Is_Sent_And_Handled_By_Handler()
    {
        // Arrange
        SimpleTestCommandHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var sender = host.Services.GetRequiredService<ICommandSender>();

        // Act
        await sender.SendAsync(new SimpleTestCommand { Action = "TestAction" });
        await SimpleTestCommandHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(10));

        // Assert
        Assert.Equal(1, SimpleTestCommandHandler.HandleCount);
        Assert.Single(SimpleTestCommandHandler.HandledCommands);
        Assert.Equal("TestAction", SimpleTestCommandHandler.HandledCommands.First().Action);
    }

    [Fact]
    public async Task Command_With_Complex_Payload_Is_Handled_Correctly()
    {
        // Arrange
        ProcessOrderCommandHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var sender = host.Services.GetRequiredService<ICommandSender>();

        var orderId = Guid.NewGuid();
        var command = new ProcessOrderCommand
        {
            OrderId = orderId,
            CustomerName = "John Doe",
            Total = 99.99m
        };

        // Act
        await sender.SendAsync(command);
        await ProcessOrderCommandHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(10));

        // Assert
        Assert.Equal(1, ProcessOrderCommandHandler.HandleCount);
        var processedOrder = ProcessOrderCommandHandler.ProcessedOrders.First();
        Assert.Equal(orderId, processedOrder.OrderId);
        Assert.Equal("John Doe", processedOrder.CustomerName);
        Assert.Equal(99.99m, processedOrder.Total);
    }

    [Fact]
    public async Task Multiple_Commands_Are_Processed_In_Order()
    {
        // Arrange
        SimpleTestCommandHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var sender = host.Services.GetRequiredService<ICommandSender>();
        const int commandCount = 10;

        // Act
        for (int i = 0; i < commandCount; i++)
        {
            await sender.SendAsync(new SimpleTestCommand { Action = $"Action-{i}" });
        }
        
        await SimpleTestCommandHandler.WaitForCountAsync(commandCount, TimeSpan.FromSeconds(30));

        // Assert
        Assert.Equal(commandCount, SimpleTestCommandHandler.HandleCount);
    }

    [Fact]
    public async Task Commands_Are_Sent_To_Correct_Queue()
    {
        // Arrange
        SimpleTestCommandHandler.Reset();
        ProcessOrderCommandHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var sender = host.Services.GetRequiredService<ICommandSender>();

        // Act - Send both command types
        await sender.SendAsync(new SimpleTestCommand { Action = "Test" });
        await sender.SendAsync(new ProcessOrderCommand { OrderId = Guid.NewGuid(), CustomerName = "Test" });
        
        await SimpleTestCommandHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(10));
        await ProcessOrderCommandHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(10));

        // Assert - Each handler receives its own command type
        Assert.Equal(1, SimpleTestCommandHandler.HandleCount);
        Assert.Equal(1, ProcessOrderCommandHandler.HandleCount);
    }

    [Fact]
    public async Task High_Volume_Commands_Are_Processed_Successfully()
    {
        // Arrange
        SimpleTestCommandHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var sender = host.Services.GetRequiredService<ICommandSender>();
        const int commandCount = 50;

        // Act
        var sendTasks = Enumerable.Range(0, commandCount)
            .Select(i => sender.SendAsync(new SimpleTestCommand { Action = $"Bulk-{i}" }));
        
        await Task.WhenAll(sendTasks);
        await SimpleTestCommandHandler.WaitForCountAsync(commandCount, TimeSpan.FromSeconds(60));

        // Assert
        Assert.Equal(commandCount, SimpleTestCommandHandler.HandleCount);
    }

    [Fact]
    public async Task Command_Id_Is_Unique_And_Preserved()
    {
        // Arrange
        SimpleTestCommandHandler.Reset();
        using var host = await BuildHostWithHandlers();
        var sender = host.Services.GetRequiredService<ICommandSender>();

        var command = new SimpleTestCommand { Action = "UniqueId" };
        var originalId = command.Id;

        // Act
        await sender.SendAsync(command);
        await SimpleTestCommandHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(10));

        // Assert
        Assert.Equal(1, SimpleTestCommandHandler.HandleCount);
        Assert.Equal(originalId, SimpleTestCommandHandler.HandledCommands.First().Id);
    }

    private async Task<IHost> BuildHostWithHandlers()
    {
        return await BuildHost(services =>
        {
            services.AddRabbitMqMessaging(options =>
            {
                options.UseHost(_rabbitMqContainer!.Hostname);
                options.UsePort(_rabbitMqContainer!.GetMappedPublicPort(5672));
                options.WithCredentials("guest", "guest");
                options.WithConnectionName($"CommandTests-{Guid.NewGuid():N}");
            })
            .AddTopology(topology => topology
                .WithServiceName("command-test-service")
                .ScanAssemblyContaining<SimpleTestCommandHandler>());
        });
    }

    protected override void ConfigureAdditionalServices(IServiceCollection services)
    {
        // No additional services needed
    }
}
