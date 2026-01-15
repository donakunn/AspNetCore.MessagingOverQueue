using Donakunn.MessagingOverQueue.RedisStreams.Configuration;
using Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;
using MessagingOverQueue.Test.Integration.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Testcontainers.Redis;
using static Donakunn.MessagingOverQueue.Topology.DependencyInjection.TopologyServiceCollectionExtensions;

namespace MessagingOverQueue.Test.Integration.RedisStreams.Infrastructure;

/// <summary>
/// Base class for Redis Streams integration tests providing containerized Redis infrastructure.
/// Uses xUnit's IAsyncLifetime for proper async setup/teardown.
/// Includes isolated test execution context and Redis-specific helpers.
/// </summary>
public abstract class RedisStreamsIntegrationTestBase : IAsyncLifetime
{
    private RedisContainer? _redisContainer;
    protected IServiceProvider ServiceProvider { get; private set; } = null!;
    protected string RedisConnectionString { get; private set; } = string.Empty;
    protected IConnectionMultiplexer? RedisConnection { get; private set; }

    private readonly TestExecutionContext _testContext;
    private readonly string _streamPrefix = $"test-{Guid.NewGuid():N}";

    protected RedisStreamsIntegrationTestBase()
    {
        // Create and set isolated test execution context for this test instance
        _testContext = new TestExecutionContext();
        TestExecutionContextAccessor.Current = _testContext;
    }

    /// <summary>
    /// Gets the test execution context for this test.
    /// </summary>
    protected TestExecutionContext TestContext => _testContext;

    /// <summary>
    /// Default timeout for waiting on async operations in tests.
    /// </summary>
    protected virtual TimeSpan DefaultTimeout => TimeSpan.FromSeconds(30);

    /// <summary>
    /// Polling interval for async operation checks.
    /// </summary>
    protected virtual TimeSpan PollingInterval => TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Redis stream prefix for test isolation.
    /// </summary>
    protected virtual string StreamPrefix => _streamPrefix;

    public async Task InitializeAsync()
    {
        // Start Redis container
        _redisContainer = new RedisBuilder()
            .WithImage("redis:7-alpine")
            .Build();

        await _redisContainer.StartAsync();
        RedisConnectionString = _redisContainer.GetConnectionString();

        // Create Redis connection for test helpers
        RedisConnection = await ConnectionMultiplexer.ConnectAsync(RedisConnectionString);

        // Setup services
        var services = new ServiceCollection();
        ConfigureServices(services);
        ServiceProvider = services.BuildServiceProvider();

        // Additional setup
        await OnInitializeAsync();
    }

    protected virtual void ConfigureServices(IServiceCollection services)
    {
        // Add logging
        services.AddLogging(builder => builder
            .SetMinimumLevel(LogLevel.Debug)
            .AddDebug());

        // Add custom services
        ConfigureAdditionalServices(services);
    }

    protected abstract void ConfigureAdditionalServices(IServiceCollection services);

    protected virtual async Task OnInitializeAsync()
    {
        await Task.CompletedTask;
    }

    protected async Task<T> ExecuteInScopeAsync<T>(Func<IServiceProvider, Task<T>> action)
    {
        using var scope = ServiceProvider.CreateScope();
        return await action(scope.ServiceProvider);
    }

    protected async Task ExecuteInScopeAsync(Func<IServiceProvider, Task> action)
    {
        using var scope = ServiceProvider.CreateScope();
        await action(scope.ServiceProvider);
    }

    /// <summary>
    /// Waits for a condition to become true with timeout.
    /// </summary>
    protected async Task WaitForConditionAsync(
        Func<bool> condition,
        TimeSpan? timeout = null,
        string? timeoutMessage = null)
    {
        var actualTimeout = timeout ?? DefaultTimeout;
        var sw = System.Diagnostics.Stopwatch.StartNew();

        while (!condition() && sw.Elapsed < actualTimeout)
        {
            await Task.Delay(PollingInterval);
        }

        if (!condition())
        {
            throw new TimeoutException(timeoutMessage ??
                $"Condition was not met within {actualTimeout.TotalSeconds} seconds.");
        }
    }

    /// <summary>
    /// Waits for an async condition to become true with timeout.
    /// </summary>
    protected async Task WaitForConditionAsync(
        Func<Task<bool>> conditionAsync,
        TimeSpan? timeout = null,
        string? timeoutMessage = null)
    {
        var actualTimeout = timeout ?? DefaultTimeout;
        var sw = System.Diagnostics.Stopwatch.StartNew();

        while (!await conditionAsync() && sw.Elapsed < actualTimeout)
        {
            await Task.Delay(PollingInterval);
        }

        if (!await conditionAsync())
        {
            throw new TimeoutException(timeoutMessage ??
                $"Condition was not met within {actualTimeout.TotalSeconds} seconds.");
        }
    }

    public async Task DisposeAsync()
    {
        await OnDisposeAsync();

        if (ServiceProvider is IAsyncDisposable asyncDisposable)
        {
            await asyncDisposable.DisposeAsync();
        }
        else if (ServiceProvider is IDisposable disposable)
        {
            disposable.Dispose();
        }

        if (RedisConnection != null)
        {
            await RedisConnection.DisposeAsync();
        }

        if (_redisContainer != null)
        {
            await _redisContainer.DisposeAsync();
        }

        // Clean up test execution context
        _testContext.Reset();
        TestExecutionContextAccessor.Current = null;
    }

    protected virtual async Task OnDisposeAsync()
    {
        await Task.CompletedTask;
    }

    /// <summary>
    /// Builds a host configured with Redis Streams messaging and topology.
    /// Handler scanning is done automatically via AddTopology.
    /// </summary>
    protected async Task<IHost> BuildHost<THandlerMarker>(Action<RedisStreamsOptionsBuilder>? configureRedis = null)
    {
        var hostBuilder = Host.CreateDefaultBuilder()
            .ConfigureServices((_, services) =>
            {
                services.AddLogging(builder => builder
                    .SetMinimumLevel(LogLevel.Debug)
                    .AddDebug());

                services.AddRedisStreamsMessaging(options =>
                {
                    options.UseConnectionString(RedisConnectionString);
                    options.WithStreamPrefix(StreamPrefix);
                    options.ConfigureConsumer(batchSize: 10, blockingTimeout: TimeSpan.FromMilliseconds(100)); // Short polling for tests
                    options.ConfigureClaiming(claimIdleTime: TimeSpan.FromSeconds(5), checkInterval: TimeSpan.FromMilliseconds(500)); // Fast claiming for tests
                    options.WithCountBasedRetention(10000);

                    configureRedis?.Invoke(options);
                })
                .AddTopology(topology => topology
                    .WithServiceName("test-service")
                    .ScanAssemblyContaining<THandlerMarker>())
                .AddRedisStreamsConsumerHostedService(); // Must be after AddTopology to avoid deadlock

                ConfigureHostServices(services);
            });

        var host = hostBuilder.Build();
        await host.StartAsync();
        return host;
    }

    /// <summary>
    /// Builds a host with custom configuration.
    /// </summary>
    protected async Task<IHost> BuildHost(Action<IServiceCollection> configureServices)
    {
        var hostBuilder = Host.CreateDefaultBuilder()
            .ConfigureServices((_, services) =>
            {
                services.AddLogging(builder => builder
                    .SetMinimumLevel(LogLevel.Debug)
                    .AddDebug());

                configureServices(services);
            });

        var host = hostBuilder.Build();
        await host.StartAsync();
        return host;
    }

    /// <summary>
    /// Override to configure additional services for the host.
    /// </summary>
    protected virtual void ConfigureHostServices(IServiceCollection services)
    {
    }

    #region Redis Test Helpers

    /// <summary>
    /// Gets the Redis database for direct testing.
    /// </summary>
    protected IDatabase GetRedisDatabase() => RedisConnection!.GetDatabase();

    /// <summary>
    /// Gets all messages from a Redis Stream.
    /// </summary>
    protected async Task<StreamEntry[]> GetStreamMessagesAsync(string streamKey)
    {
        var db = GetRedisDatabase();
        return await db.StreamRangeAsync(streamKey);
    }

    /// <summary>
    /// Gets the length of a Redis Stream.
    /// </summary>
    protected async Task<long> GetStreamLengthAsync(string streamKey)
    {
        var db = GetRedisDatabase();
        return await db.StreamLengthAsync(streamKey);
    }

    /// <summary>
    /// Checks if a consumer group exists on a stream.
    /// </summary>
    protected async Task<bool> ConsumerGroupExistsAsync(string streamKey, string consumerGroup)
    {
        try
        {
            var db = GetRedisDatabase();
            var groups = await db.StreamGroupInfoAsync(streamKey);
            return groups.Any(g => g.Name == consumerGroup);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("no such key"))
        {
            return false;
        }
    }

    /// <summary>
    /// Gets pending messages count for a consumer group.
    /// </summary>
    protected async Task<long> GetPendingMessagesCountAsync(string streamKey, string consumerGroup)
    {
        try
        {
            var db = GetRedisDatabase();
            var pending = await db.StreamPendingAsync(streamKey, consumerGroup);
            return pending.PendingMessageCount;
        }
        catch
        {
            return 0;
        }
    }

    /// <summary>
    /// Gets consumers in a consumer group.
    /// </summary>
    protected async Task<StreamConsumerInfo[]> GetConsumersAsync(string streamKey, string consumerGroup)
    {
        try
        {
            var db = GetRedisDatabase();
            return await db.StreamConsumerInfoAsync(streamKey, consumerGroup);
        }
        catch
        {
            return [];
        }
    }

    /// <summary>
    /// Manually adds a message to a stream (for testing).
    /// </summary>
    protected async Task<RedisValue> AddStreamMessageAsync(string streamKey, Dictionary<string, string> fields)
    {
        var db = GetRedisDatabase();
        var entries = fields.Select(kvp => new NameValueEntry(kvp.Key, kvp.Value)).ToArray();
        return await db.StreamAddAsync(streamKey, entries);
    }

    /// <summary>
    /// Deletes a stream (cleanup).
    /// </summary>
    protected async Task DeleteStreamAsync(string streamKey)
    {
        var db = GetRedisDatabase();
        await db.KeyDeleteAsync(streamKey);
    }

    /// <summary>
    /// Gets all keys matching a pattern.
    /// </summary>
    protected async Task<RedisKey[]> GetKeysAsync(string pattern)
    {
        var server = RedisConnection!.GetServer(RedisConnection.GetEndPoints().First());
        var keys = new List<RedisKey>();

        await foreach (var key in server.KeysAsync(pattern: pattern))
        {
            keys.Add(key);
        }

        return keys.ToArray();
    }

    #endregion
}
