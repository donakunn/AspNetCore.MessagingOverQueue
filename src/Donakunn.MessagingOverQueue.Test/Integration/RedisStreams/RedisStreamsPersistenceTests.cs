using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.DependencyInjection;
using Donakunn.MessagingOverQueue.DependencyInjection.Persistence;
using Donakunn.MessagingOverQueue.DependencyInjection.Resilience;
using Donakunn.MessagingOverQueue.Persistence;
using Donakunn.MessagingOverQueue.Persistence.Entities;
using Donakunn.MessagingOverQueue.Persistence.Providers;
using Donakunn.MessagingOverQueue.Persistence.Repositories;
using Donakunn.MessagingOverQueue.RedisStreams.Configuration;
using Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection.Queues;
using MessagingOverQueue.Test.Integration.Shared.Infrastructure;
using MessagingOverQueue.Test.Integration.Shared.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Testcontainers.MsSql;

namespace MessagingOverQueue.Test.Integration.RedisStreams;

/// <summary>
/// Integration tests for Redis Streams with persistence patterns using the new fluent API.
/// Tests outbox pattern and idempotency middleware integration.
/// </summary>
public class RedisStreamsPersistenceTests : IAsyncLifetime
{
    private MsSqlContainer? _sqlContainer;
    private Testcontainers.Redis.RedisContainer? _redisContainer;
    protected IServiceProvider ServiceProvider { get; private set; } = null!;
    protected string ConnectionString { get; private set; } = string.Empty;
    protected string RedisConnectionString { get; private set; } = string.Empty;

    private readonly TestExecutionContext _testContext;
    private readonly string _streamPrefix = $"test-{Guid.NewGuid():N}";

    public RedisStreamsPersistenceTests()
    {
        _testContext = new TestExecutionContext();
        TestExecutionContextAccessor.Current = _testContext;
    }

    protected TestExecutionContext TestContext => _testContext;
    protected string StreamPrefix => _streamPrefix;

    public async Task InitializeAsync()
    {
        // Start SQL Server container
        _sqlContainer = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("YourStrong@Passw0rd")
            .Build();

        await _sqlContainer.StartAsync();
        ConnectionString = _sqlContainer.GetConnectionString();

        // Start Redis container
        _redisContainer = new Testcontainers.Redis.RedisBuilder()
            .WithImage("redis:7-alpine")
            .Build();

        await _redisContainer.StartAsync();
        RedisConnectionString = _redisContainer.GetConnectionString();

        var services = new ServiceCollection();
        services.AddLogging();
        ServiceProvider = services.BuildServiceProvider();
    }

    public async Task DisposeAsync()
    {
        if (ServiceProvider is IAsyncDisposable asyncDisposable)
        {
            await asyncDisposable.DisposeAsync();
        }
        else if (ServiceProvider is IDisposable disposable)
        {
            disposable.Dispose();
        }

        if (_sqlContainer != null)
        {
            await _sqlContainer.DisposeAsync();
        }

        if (_redisContainer != null)
        {
            await _redisContainer.DisposeAsync();
        }

        _testContext.Reset();
        TestExecutionContextAccessor.Current = null;
    }

    [Fact]
    public async Task UsePersistence_WithOutbox_ConfiguresOutboxServices()
    {
        // Arrange & Act
        using var host = await BuildHostWithPersistence(persistence => persistence
            .WithOutbox(opts => opts.BatchSize = 50));

        var provider = host.Services;

        // Assert - Verify outbox services are registered
        Assert.NotNull(provider.GetService<IOutboxRepository>());
        Assert.NotNull(provider.GetService<IInboxRepository>());

        // Assert - Verify outbox options are configured
        var outboxOptions = provider.GetRequiredService<IOptions<OutboxOptions>>().Value;
        Assert.Equal(50, outboxOptions.BatchSize);
    }

    [Fact]
    public async Task UsePersistence_WithIdempotency_ConfiguresIdempotencyMiddleware()
    {
        // Arrange & Act
        using var host = await BuildHostWithPersistence(persistence =>
        {
            persistence.WithOutbox();
            persistence.WithIdempotency(opts => opts.RetentionPeriod = TimeSpan.FromDays(14));
        });

        var provider = host.Services;

        // Assert - Verify idempotency middleware is registered
        var middlewares = provider.GetServices<IConsumeMiddleware>().ToList();
        Assert.Contains(middlewares, m => m is IdempotencyMiddleware);

        // Assert - Verify idempotency options are configured
        var idempotencyOptions = provider.GetRequiredService<IOptions<IdempotencyOptions>>().Value;
        Assert.Equal(TimeSpan.FromDays(14), idempotencyOptions.RetentionPeriod);
    }

    [Fact]
    public async Task UsePersistence_FullConfig_RegistersAllServices()
    {
        // Arrange & Act
        using var host = await BuildHostWithPersistence(persistence =>
        {
            persistence.WithOutbox(opts =>
            {
                opts.BatchSize = 100;
                opts.ProcessingInterval = TimeSpan.FromSeconds(1);
                opts.Enabled = true;
                opts.AutoCleanup = true;
            });
            persistence.WithIdempotency(opts =>
            {
                opts.Enabled = true;
                opts.RetentionPeriod = TimeSpan.FromDays(7);
            });
        });

        var provider = host.Services;

        // Assert - Verify all services are registered
        Assert.NotNull(provider.GetService<IOutboxRepository>());
        Assert.NotNull(provider.GetService<IInboxRepository>());
        Assert.NotNull(provider.GetService<IMessageStoreProvider>());

        // Assert - Verify middleware is registered
        var middlewares = provider.GetServices<IConsumeMiddleware>().ToList();
        Assert.Contains(middlewares, m => m is IdempotencyMiddleware);
    }

    [Fact]
    public async Task OutboxPublisher_StoresMessageInDatabase_WithRedisStreams()
    {
        // Arrange
        using var host = await BuildHostWithPersistence(persistence => persistence
            .WithOutbox(opts => opts.Enabled = true));

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
    public async Task Idempotency_DuplicateMessages_ProcessedOnce()
    {
        // Arrange
        IdempotentTestEventHandler.Reset();

        using var host = await BuildHostWithPersistence(persistence =>
        {
            persistence.WithOutbox(opts =>
            {
                opts.Enabled = true;
                opts.AutoCreateSchema = true;
            });
            persistence.WithIdempotency();
        });

        var testEvent = new IdempotentTestEvent { Value = "IdempotencyTest" };

        // Act - publish the same message multiple times directly to Redis
        using (var scope = host.Services.CreateScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<IEventPublisher>();

            // Publish same message 3 times
            await publisher.PublishAsync(testEvent);
            await publisher.PublishAsync(testEvent);
            await publisher.PublishAsync(testEvent);
        }

        // Wait for first message to be processed
        await IdempotentTestEventHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(15));

        // Give time for any additional messages to be processed
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Assert - handler should only have been called once
        Assert.Equal(1, IdempotentTestEventHandler.HandleCount);
    }

    [Fact]
    public async Task InboxRepository_TracksProcessedMessages_WithRedisStreams()
    {
        // Arrange
        using var host = await BuildHostWithPersistence(persistence => persistence
            .WithOutbox(opts => opts.AutoCreateSchema = true));

        var testEvent = new SimpleTestEvent { Value = "InboxTest" };
        var handlerType = typeof(SimpleTestEventHandler).FullName!;

        // Act - mark message as processed
        using (var scope = host.Services.CreateScope())
        {
            var inboxRepository = scope.ServiceProvider.GetRequiredService<IInboxRepository>();
            await inboxRepository.MarkAsProcessedAsync(testEvent, handlerType);
        }

        // Assert - verify message is in inbox
        using (var scope = host.Services.CreateScope())
        {
            var inboxRepository = scope.ServiceProvider.GetRequiredService<IInboxRepository>();
            var hasBeenProcessed = await inboxRepository.HasBeenProcessedAsync(testEvent.Id, handlerType);

            Assert.True(hasBeenProcessed);
        }
    }

    [Fact]
    public async Task UsePersistence_CombinedWithResilience_WorksTogether()
    {
        // Arrange
        SimpleTestEventHandler.Reset();

        using var host = await BuildHostWithPersistenceAndResilience();

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Combined Test" });
        await SimpleTestEventHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(15));

        // Assert
        Assert.Equal(1, SimpleTestEventHandler.HandleCount);

        // Verify both services are registered (use scope for scoped services)
        using var scope = host.Services.CreateScope();
        var middlewares = scope.ServiceProvider.GetServices<IConsumeMiddleware>().ToList();
        Assert.Contains(middlewares, m => m is IdempotencyMiddleware);
        Assert.Contains(middlewares, m => m is RetryMiddleware);
    }

    private async Task<IHost> BuildHostWithPersistence(Action<IPersistenceBuilder> configurePersistence)
    {
        // Capture the test context to register in the host
        var testContext = _testContext;

        var hostBuilder = Host.CreateDefaultBuilder()
            .ConfigureServices((_, services) =>
            {
                services.AddLogging();

                // Register test context as singleton so it can be resolved by middleware
                services.AddSingleton(testContext);

                // Add middleware to set the test context before each message is processed
                services.AddSingleton<IConsumeMiddleware, PersistenceTestContextPropagationMiddleware>();

                services.AddMessaging()
                    .UseRedisStreamsQueues(queues => queues
                        .WithConnection(opts =>
                        {
                            opts.UseConnectionString(RedisConnectionString);
                            opts.WithStreamPrefix(StreamPrefix);
                            opts.ConfigureConsumer(batchSize: 10, blockingTimeout: TimeSpan.FromMilliseconds(100));
                        })
                        .WithTopology(topology => topology
                            .WithServiceName("persistence-test-service")
                            .ScanAssemblyContaining<SimpleTestEventHandler>()))
                    .UsePersistence(persistence =>
                    {
                        configurePersistence(persistence);
                    });

                // Use SQL Server for the message store
                services.AddSingleton<IMessageStoreProvider>(sp =>
                {
                    // Use InMemoryMessageStoreProvider for faster tests
                    return new InMemoryMessageStoreProvider();
                });
            });

        var host = hostBuilder.Build();
        await host.StartAsync();
        return host;
    }

    private async Task<IHost> BuildHostWithPersistenceAndResilience()
    {
        // Capture the test context to register in the host
        var testContext = _testContext;

        var hostBuilder = Host.CreateDefaultBuilder()
            .ConfigureServices((_, services) =>
            {
                services.AddLogging();

                // Register test context as singleton so it can be resolved by middleware
                services.AddSingleton(testContext);

                // Add middleware to set the test context before each message is processed
                services.AddSingleton<IConsumeMiddleware, PersistenceTestContextPropagationMiddleware>();

                services.AddMessaging()
                    .UseRedisStreamsQueues(queues => queues
                        .WithConnection(opts =>
                        {
                            opts.UseConnectionString(RedisConnectionString);
                            opts.WithStreamPrefix(StreamPrefix);
                            opts.ConfigureConsumer(batchSize: 10, blockingTimeout: TimeSpan.FromMilliseconds(100));
                        })
                        .WithTopology(topology => topology
                            .WithServiceName("combined-test-service")
                            .ScanAssemblyContaining<SimpleTestEventHandler>()))
                    .UseResilience(resilience => resilience
                        .WithRetry(opts => opts.MaxRetryAttempts = 3))
                    .UsePersistence(persistence =>
                    {
                        persistence.WithOutbox(opts => opts.AutoCreateSchema = true);
                        persistence.WithIdempotency();
                    });

                // Use InMemoryMessageStoreProvider for faster tests
                services.AddSingleton<IMessageStoreProvider, InMemoryMessageStoreProvider>();
            });

        var host = hostBuilder.Build();
        await host.StartAsync();
        return host;
    }
}

/// <summary>
/// Middleware that propagates the test execution context to the consumer's async flow.
/// </summary>
internal sealed class PersistenceTestContextPropagationMiddleware : IConsumeMiddleware
{
    private readonly TestExecutionContext _testContext;

    public PersistenceTestContextPropagationMiddleware(TestExecutionContext testContext)
    {
        _testContext = testContext;
    }

    public async Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
    {
        var previousContext = TestExecutionContextAccessor.Current;
        TestExecutionContextAccessor.Current = _testContext;
        try
        {
            await next(context, cancellationToken);
        }
        finally
        {
            TestExecutionContextAccessor.Current = previousContext;
        }
    }
}
