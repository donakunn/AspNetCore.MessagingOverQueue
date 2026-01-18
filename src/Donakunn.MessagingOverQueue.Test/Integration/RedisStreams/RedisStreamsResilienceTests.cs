using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.DependencyInjection;
using Donakunn.MessagingOverQueue.DependencyInjection.Resilience;
using Donakunn.MessagingOverQueue.RedisStreams.Configuration;
using Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection.Queues;
using Donakunn.MessagingOverQueue.Resilience;
using Donakunn.MessagingOverQueue.Resilience.CircuitBreaker;
using MessagingOverQueue.Test.Integration.RedisStreams.Infrastructure;
using MessagingOverQueue.Test.Integration.Shared.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace MessagingOverQueue.Test.Integration.RedisStreams;

/// <summary>
/// Integration tests for Redis Streams with resilience patterns using the new fluent API.
/// Tests retry, circuit breaker, and timeout middleware integration.
/// </summary>
public class RedisStreamsResilienceTests : RedisStreamsIntegrationTestBase
{
    protected override void ConfigureAdditionalServices(IServiceCollection services)
    {
        // No additional services needed for basic tests
    }

    [Fact]
    public async Task UseResilience_WithRetry_ConfiguresRetryMiddleware()
    {
        // Arrange & Act
        using var host = await BuildHostWithResilience(resilience => resilience
            .WithRetry(opts => opts.MaxRetryAttempts = 5));

        var provider = host.Services;

        // Assert - Verify retry middleware is registered
        var middlewares = provider.GetServices<IConsumeMiddleware>().ToList();
        Assert.Contains(middlewares, m => m is RetryMiddleware);

        // Assert - Verify retry options are configured
        var retryOptions = provider.GetRequiredService<IOptions<RetryOptions>>().Value;
        Assert.Equal(5, retryOptions.MaxRetryAttempts);
    }

    [Fact]
    public async Task UseResilience_WithCircuitBreaker_ConfiguresCircuitBreakerMiddleware()
    {
        // Arrange & Act
        using var host = await BuildHostWithResilience(resilience => resilience
            .WithCircuitBreaker(opts => opts.FailureThreshold = 10));

        var provider = host.Services;

        // Assert - Verify circuit breaker middleware is registered
        var middlewares = provider.GetServices<IConsumeMiddleware>().ToList();
        Assert.Contains(middlewares, m => m is CircuitBreakerMiddleware);

        // Assert - Verify circuit breaker is registered
        var circuitBreaker = provider.GetService<ICircuitBreaker>();
        Assert.NotNull(circuitBreaker);
    }

    [Fact]
    public async Task UseResilience_WithTimeout_ConfiguresTimeoutMiddleware()
    {
        // Arrange & Act
        using var host = await BuildHostWithResilience(resilience => resilience
            .WithTimeout(TimeSpan.FromSeconds(30)));

        var provider = host.Services;

        // Assert - Verify timeout middleware is registered
        var middlewares = provider.GetServices<IConsumeMiddleware>().ToList();
        Assert.Contains(middlewares, m => m is TimeoutMiddleware);

        // Assert - Verify timeout options are configured
        var timeoutOptions = provider.GetRequiredService<IOptions<TimeoutOptions>>().Value;
        Assert.Equal(TimeSpan.FromSeconds(30), timeoutOptions.Timeout);
    }

    [Fact]
    public async Task UseResilience_FullConfig_RegistersAllMiddleware()
    {
        // Arrange & Act
        using var host = await BuildHostWithResilience(resilience => resilience
            .WithRetry(opts => opts.MaxRetryAttempts = 3)
            .WithCircuitBreaker(opts => opts.FailureThreshold = 5)
            .WithTimeout(TimeSpan.FromSeconds(15)));

        var provider = host.Services;

        // Assert - Verify all middleware is registered
        var middlewares = provider.GetServices<IConsumeMiddleware>().ToList();
        Assert.Contains(middlewares, m => m is RetryMiddleware);
        Assert.Contains(middlewares, m => m is CircuitBreakerMiddleware);
        Assert.Contains(middlewares, m => m is TimeoutMiddleware);
    }

    [Fact]
    public async Task UseResilience_MessageProcessingWithRetry_Works()
    {
        // Arrange
        SimpleTestEventHandler.Reset();

        using var host = await BuildHostWithResilience(resilience => resilience
            .WithRetry(opts => opts.MaxRetryAttempts = 3));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        // Act
        await publisher.PublishAsync(new SimpleTestEvent { Value = "Retry Test" });
        await SimpleTestEventHandler.WaitForCountAsync(1, TimeSpan.FromSeconds(10));

        // Assert
        Assert.Equal(1, SimpleTestEventHandler.HandleCount);
        Assert.Equal("Retry Test", SimpleTestEventHandler.HandledMessages.First().Value);
    }

    [Fact]
    public async Task UseResilience_MiddlewareOrderingCorrect()
    {
        // Arrange & Act
        using var host = await BuildHostWithResilience(resilience => resilience
            .WithRetry()
            .WithCircuitBreaker()
            .WithTimeout(TimeSpan.FromSeconds(30)));

        var provider = host.Services;

        // Assert - Get all ordered middlewares
        var middlewares = provider.GetServices<IConsumeMiddleware>()
            .OfType<IOrderedConsumeMiddleware>()
            .OrderBy(m => m.Order)
            .ToList();

        // Verify ordering: CircuitBreaker (100) < Retry (200) < Timeout (300)
        var circuitBreaker = middlewares.FirstOrDefault(m => m is CircuitBreakerMiddleware);
        var retry = middlewares.FirstOrDefault(m => m is RetryMiddleware);
        var timeout = middlewares.FirstOrDefault(m => m is TimeoutMiddleware);

        Assert.NotNull(circuitBreaker);
        Assert.NotNull(retry);
        Assert.NotNull(timeout);

        Assert.True(circuitBreaker!.Order < retry!.Order);
        Assert.True(retry!.Order < timeout!.Order);
    }

    [Fact]
    public async Task UseResilience_MultipleConcurrentMessages_ProcessedSuccessfully()
    {
        // Arrange
        SimpleTestEventHandler.Reset();

        using var host = await BuildHostWithResilience(resilience => resilience
            .WithRetry(opts => opts.MaxRetryAttempts = 3)
            .WithTimeout(TimeSpan.FromSeconds(30)));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();
        const int messageCount = 20;

        // Act
        var publishTasks = Enumerable.Range(0, messageCount)
            .Select(i => publisher.PublishAsync(new SimpleTestEvent { Value = $"Concurrent-{i}" }));

        await Task.WhenAll(publishTasks);
        await SimpleTestEventHandler.WaitForCountAsync(messageCount, TimeSpan.FromSeconds(30));

        // Assert
        Assert.Equal(messageCount, SimpleTestEventHandler.HandleCount);
    }

    private async Task<IHost> BuildHostWithResilience(Action<IResilienceBuilder> configureResilience)
    {
        // Capture the test context to register in the host
        var testContext = TestContext;

        var hostBuilder = Host.CreateDefaultBuilder()
            .ConfigureServices((_, services) =>
            {
                services.AddLogging();

                // Register test context as singleton so it can be resolved by middleware
                services.AddSingleton(testContext);

                // Add middleware to set the test context before each message is processed
                services.AddSingleton<IConsumeMiddleware, TestContextPropagationMiddleware>();

                services.AddMessaging()
                    .UseRedisStreamsQueues(queues => queues
                        .WithConnection(opts =>
                        {
                            opts.UseConnectionString(RedisConnectionString);
                            opts.WithStreamPrefix(StreamPrefix);
                            opts.ConfigureConsumer(batchSize: 10, blockingTimeout: TimeSpan.FromMilliseconds(100));
                        })
                        .WithTopology(topology => topology
                            .WithServiceName("resilience-test-service")
                            .ScanAssemblyContaining<SimpleTestEventHandler>()))
                    .UseResilience(configureResilience);
            });

        var host = hostBuilder.Build();
        await host.StartAsync();
        return host;
    }
}

/// <summary>
/// Middleware that propagates the test execution context to the consumer's async flow.
/// </summary>
internal sealed class TestContextPropagationMiddleware : IConsumeMiddleware
{
    private readonly Shared.Infrastructure.TestExecutionContext _testContext;

    public TestContextPropagationMiddleware(Shared.Infrastructure.TestExecutionContext testContext)
    {
        _testContext = testContext;
    }

    public async Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
    {
        var previousContext = Shared.Infrastructure.TestExecutionContextAccessor.Current;
        Shared.Infrastructure.TestExecutionContextAccessor.Current = _testContext;
        try
        {
            await next(context, cancellationToken);
        }
        finally
        {
            Shared.Infrastructure.TestExecutionContextAccessor.Current = previousContext;
        }
    }
}
