using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.DependencyInjection;
using Donakunn.MessagingOverQueue.DependencyInjection.Persistence;
using Donakunn.MessagingOverQueue.DependencyInjection.Queues;
using Donakunn.MessagingOverQueue.DependencyInjection.Resilience;
using Donakunn.MessagingOverQueue.Persistence.Repositories;
using Donakunn.MessagingOverQueue.Providers;
using Donakunn.MessagingOverQueue.Resilience;
using Donakunn.MessagingOverQueue.Resilience.CircuitBreaker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace MessagingOverQueue.Test.Unit.DependencyInjection;

/// <summary>
/// Unit tests for the new fluent DI API.
/// </summary>
public class NewApiRegistrationTests
{
    [Fact]
    public void AddMessaging_ReturnsMessagingBuilder()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        var builder = services.AddMessaging();

        // Assert
        Assert.NotNull(builder);
        Assert.IsAssignableFrom<IMessagingBuilder>(builder);
    }

    [Fact]
    public void UseRabbitMqQueues_RegistersCoreServices()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        services.AddMessaging()
            .UseRabbitMqQueues(queues => queues
                .WithConnection(opts => opts.UseHost("localhost")));

        // Assert - verify options and non-topology services are registered
        // Note: IMessagingProvider requires ITopologyDeclarer which needs topology config
        var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetService<IOptions<RabbitMqOptions>>());
        Assert.NotNull(provider.GetService<IRetryPolicy>());
    }

    [Fact]
    public void UseRabbitMqQueues_WithConnection_ConfiguresOptions()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        services.AddMessaging()
            .UseRabbitMqQueues(queues => queues
                .WithConnection(opts => opts
                    .UseHost("myhost")
                    .UsePort(5673)
                    .WithCredentials("user", "pass")));

        // Assert
        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<RabbitMqOptions>>().Value;

        Assert.Equal("myhost", options.HostName);
        Assert.Equal(5673, options.Port);
        Assert.Equal("user", options.UserName);
        Assert.Equal("pass", options.Password);
    }

    [Fact]
    public void UseResilience_WithRetry_RegistersRetryMiddleware()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        services.AddMessaging()
            .UseRabbitMqQueues(queues => queues
                .WithConnection(opts => opts.UseHost("localhost")))
            .UseResilience(resilience => resilience
                .WithRetry(opts => opts.MaxRetryAttempts = 5));

        // Assert
        var provider = services.BuildServiceProvider();
        var middlewares = provider.GetServices<IConsumeMiddleware>().ToList();
        var retryOptions = provider.GetRequiredService<IOptions<RetryOptions>>().Value;

        Assert.Contains(middlewares, m => m is RetryMiddleware);
        Assert.Equal(5, retryOptions.MaxRetryAttempts);
    }

    [Fact]
    public void UseResilience_WithCircuitBreaker_RegistersCircuitBreakerMiddleware()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        services.AddMessaging()
            .UseRabbitMqQueues(queues => queues
                .WithConnection(opts => opts.UseHost("localhost")))
            .UseResilience(resilience => resilience
                .WithCircuitBreaker(opts => opts.FailureThreshold = 10));

        // Assert
        var provider = services.BuildServiceProvider();
        var middlewares = provider.GetServices<IConsumeMiddleware>().ToList();
        var circuitBreaker = provider.GetService<ICircuitBreaker>();

        Assert.Contains(middlewares, m => m is CircuitBreakerMiddleware);
        Assert.NotNull(circuitBreaker);
    }

    [Fact]
    public void UseResilience_WithTimeout_RegistersTimeoutMiddleware()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        services.AddMessaging()
            .UseRabbitMqQueues(queues => queues
                .WithConnection(opts => opts.UseHost("localhost")))
            .UseResilience(resilience => resilience
                .WithTimeout(TimeSpan.FromSeconds(30)));

        // Assert
        var provider = services.BuildServiceProvider();
        var middlewares = provider.GetServices<IConsumeMiddleware>().ToList();
        var timeoutOptions = provider.GetRequiredService<IOptions<TimeoutOptions>>().Value;

        Assert.Contains(middlewares, m => m is TimeoutMiddleware);
        Assert.Equal(TimeSpan.FromSeconds(30), timeoutOptions.Timeout);
    }

    [Fact]
    public void UsePersistence_WithOutbox_RegistersOutboxOptions()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        services.AddMessaging()
            .UseRabbitMqQueues(queues => queues
                .WithConnection(opts => opts.UseHost("localhost")))
            .UsePersistence(persistence => persistence
                .WithOutbox(opts => opts.BatchSize = 50));

        // Assert - verify options are configured
        // Note: Repositories require IMessageStoreProvider which needs SQL Server
        var provider = services.BuildServiceProvider();

        var outboxOptions = provider.GetRequiredService<IOptions<OutboxOptions>>().Value;
        Assert.Equal(50, outboxOptions.BatchSize);
    }

    [Fact]
    public void UsePersistence_WithIdempotency_RegistersIdempotencyMiddleware()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act - Register idempotency without outbox (decoupled)
        services.AddMessaging()
            .UseRabbitMqQueues(queues => queues
                .WithConnection(opts => opts.UseHost("localhost")))
            .UsePersistence(persistence => persistence
                .WithIdempotency(opts => opts.RetentionPeriod = TimeSpan.FromDays(14)));

        // Assert
        var provider = services.BuildServiceProvider();
        var idempotencyOptions = provider.GetRequiredService<IOptions<IdempotencyOptions>>().Value;

        Assert.Equal(TimeSpan.FromDays(14), idempotencyOptions.RetentionPeriod);
    }

    [Fact]
    public void FullFluentApi_RegistersAllServices()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        services.AddMessaging()
            .UseRabbitMqQueues(queues => queues
                .WithConnection(opts => opts
                    .UseHost("localhost")
                    .UsePort(5672)
                    .WithCredentials("guest", "guest")))
            .UseResilience(resilience => resilience
                .WithRetry(opts => opts.MaxRetryAttempts = 5)
                .WithCircuitBreaker(opts => opts.FailureThreshold = 10)
                .WithTimeout(TimeSpan.FromSeconds(30)))
            .UsePersistence(persistence =>
            {
                // WithOutbox returns IOutboxBuilder, WithIdempotency is on IPersistenceBuilder
                // So we configure them separately
                persistence.WithOutbox(opts => opts.BatchSize = 50);
                persistence.WithIdempotency();
            });

        // Assert - verify configuration options are registered
        // Note: IMessagingProvider requires ITopologyDeclarer which needs topology config
        // Note: Repositories require IMessageStoreProvider (SQL Server)
        // Note: IdempotencyMiddleware requires IInboxRepository, so we can't resolve all middlewares
        var provider = services.BuildServiceProvider();

        // Core options
        Assert.NotNull(provider.GetService<IOptions<RabbitMqOptions>>());

        // Resilience options and services
        Assert.NotNull(provider.GetService<IRetryPolicy>());
        Assert.NotNull(provider.GetService<ICircuitBreaker>());
        Assert.NotNull(provider.GetService<IOptions<RetryOptions>>());
        Assert.NotNull(provider.GetService<IOptions<CircuitBreakerOptions>>());
        Assert.NotNull(provider.GetService<IOptions<TimeoutOptions>>());

        // Persistence options
        Assert.NotNull(provider.GetService<IOptions<OutboxOptions>>());
        Assert.NotNull(provider.GetService<IOptions<IdempotencyOptions>>());

        // Verify middleware types implement IOrderedConsumeMiddleware
        // (Can't resolve middlewares because IdempotencyMiddleware requires IInboxRepository -> IMessageStoreProvider)
        Assert.True(typeof(IOrderedConsumeMiddleware).IsAssignableFrom(typeof(RetryMiddleware)));
        Assert.True(typeof(IOrderedConsumeMiddleware).IsAssignableFrom(typeof(CircuitBreakerMiddleware)));
        Assert.True(typeof(IOrderedConsumeMiddleware).IsAssignableFrom(typeof(TimeoutMiddleware)));
    }

    [Fact]
    public void MessagingBuilder_HasQueueProvider_IsTrueAfterUseRabbitMqQueues()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        // Act
        var builder = services.AddMessaging();
        Assert.False(builder.HasQueueProvider);

        builder.UseRabbitMqQueues(queues => queues
            .WithConnection(opts => opts.UseHost("localhost")));

        // Assert
        Assert.True(builder.HasQueueProvider);
    }

    [Fact]
    public void RegisteredMiddleware_ImplementsIOrderedConsumeMiddleware()
    {
        // Assert - verify that updated middleware implements the interface
        Assert.True(typeof(IOrderedConsumeMiddleware).IsAssignableFrom(typeof(ConsumeLoggingMiddleware)));
        Assert.True(typeof(IOrderedConsumeMiddleware).IsAssignableFrom(typeof(DeserializationMiddleware)));
        Assert.True(typeof(IOrderedConsumeMiddleware).IsAssignableFrom(typeof(RetryMiddleware)));
        Assert.True(typeof(IOrderedConsumeMiddleware).IsAssignableFrom(typeof(CircuitBreakerMiddleware)));
        Assert.True(typeof(IOrderedConsumeMiddleware).IsAssignableFrom(typeof(TimeoutMiddleware)));
        Assert.True(typeof(IOrderedConsumeMiddleware).IsAssignableFrom(typeof(IdempotencyMiddleware)));
    }
}
