using Donakunn.MessagingOverQueue.DependencyInjection;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Testcontainers.MsSql;
using Testcontainers.RabbitMq;
using static Donakunn.MessagingOverQueue.Topology.DependencyInjection.TopologyServiceCollectionExtensions;

namespace MessagingOverQueue.Test.Integration.Infrastructure;

/// <summary>
/// Base class for integration tests providing containerized SQL Server and RabbitMQ infrastructure.
/// Uses xUnit's IAsyncLifetime for proper async setup/teardown.
/// </summary>
public abstract class IntegrationTestBase : IAsyncLifetime
{
    private MsSqlContainer? _sqlContainer;
    protected RabbitMqContainer? _rabbitMqContainer;
    protected IServiceProvider ServiceProvider { get; private set; } = null!;
    protected string ConnectionString { get; private set; } = string.Empty;
    protected string RabbitMqConnectionString { get; private set; } = string.Empty;

    /// <summary>
    /// Default timeout for waiting on async operations in tests.
    /// </summary>
    protected virtual TimeSpan DefaultTimeout => TimeSpan.FromSeconds(30);

    /// <summary>
    /// Polling interval for async operation checks.
    /// </summary>
    protected virtual TimeSpan PollingInterval => TimeSpan.FromMilliseconds(50);

    public async Task InitializeAsync()
    {
        // Start SQL Server container
        _sqlContainer = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("YourStrong@Passw0rd")
            .Build();

        await _sqlContainer.StartAsync();
        ConnectionString = _sqlContainer.GetConnectionString();

        // Start RabbitMQ container
        _rabbitMqContainer = new RabbitMqBuilder()
            .WithImage("rabbitmq:3-management")
            .WithUsername("guest")
            .WithPassword("guest")
            .Build();

        await _rabbitMqContainer.StartAsync();
        RabbitMqConnectionString = _rabbitMqContainer.GetConnectionString();

        // Setup services
        var services = new ServiceCollection();
        ConfigureServices(services);
        ServiceProvider = services.BuildServiceProvider();

        // Apply migrations
        await ApplyMigrationsAsync();

        // Additional setup
        await OnInitializeAsync();
    }

    protected virtual void ConfigureServices(IServiceCollection services)
    {
        // Register DbContext
        services.AddDbContext<TestDbContext>(options =>
            options.UseSqlServer(ConnectionString));

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

    private async Task ApplyMigrationsAsync()
    {
        using var scope = ServiceProvider.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        await dbContext.Database.EnsureCreatedAsync();
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

    protected T GetService<T>() where T : notnull
    {
        return ServiceProvider.GetRequiredService<T>();
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

        if (_sqlContainer != null)
        {
            await _sqlContainer.DisposeAsync();
        }

        if (_rabbitMqContainer != null)
        {
            await _rabbitMqContainer.DisposeAsync();
        }
    }

    protected virtual async Task OnDisposeAsync()
    {
        await Task.CompletedTask;
    }

    /// <summary>
    /// Builds a host configured with RabbitMQ messaging and topology.
    /// Handler scanning is done automatically via AddTopology.
    /// </summary>
    protected async Task<IHost> BuildHost<THandlerMarker>()
    {
        var hostBuilder = Host.CreateDefaultBuilder()
            .ConfigureServices((_, services) =>
            {
                services.AddLogging(builder => builder
                    .SetMinimumLevel(LogLevel.Debug)
                    .AddDebug());

                services.AddRabbitMqMessaging(options =>
                {
                    options.UseHost(_rabbitMqContainer!.Hostname);
                    options.UsePort(_rabbitMqContainer!.GetMappedPublicPort(5672));
                    options.WithCredentials("guest", "guest");
                    options.WithConnectionName($"TestConnection-{Guid.NewGuid():N}");
                })
                .AddTopology(topology => topology
                    .WithServiceName("test-service")
                    .ScanAssemblyContaining<THandlerMarker>());

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
}
