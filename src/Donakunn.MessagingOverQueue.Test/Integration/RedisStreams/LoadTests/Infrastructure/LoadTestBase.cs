using System.Diagnostics;
using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.DependencyInjection;
using Donakunn.MessagingOverQueue.DependencyInjection.Persistence;
using Donakunn.MessagingOverQueue.DependencyInjection.Resilience;
using Donakunn.MessagingOverQueue.Persistence.Providers;
using Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection.Queues;
using MessagingOverQueue.Test.Integration.RedisStreams.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Metrics;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.TestMessages;
using MessagingOverQueue.Test.Integration.Shared.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.MsSql;
using Xunit.Abstractions;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Infrastructure;

/// <summary>
/// Base class for Redis Streams load tests.
/// Extends RedisStreamsIntegrationTestBase with load testing infrastructure.
/// </summary>
public abstract class LoadTestBase : RedisStreamsIntegrationTestBase
{
    private readonly ITestOutputHelper _output;
    private Task? _periodicReportingTask;
    private MsSqlContainer? _sqlContainer;

    /// <summary>
    /// SQL Server connection string (available when persistence features are enabled).
    /// </summary>
    protected string? SqlConnectionString { get; private set; }

    /// <summary>
    /// Feature configuration for this test run.
    /// </summary>
    protected FeatureConfiguration Features { get; private set; } = FeatureConfiguration.None;

    /// <summary>
    /// Load test configuration loaded from environment or defaults.
    /// </summary>
    protected LoadTestConfiguration Config { get; }

    /// <summary>
    /// Metrics aggregator for this test run.
    /// </summary>
    protected MetricsAggregator Metrics { get; }

    /// <summary>
    /// Reporter for test output.
    /// </summary>
    protected LoadTestReporter Reporter { get; }

    /// <summary>
    /// Cancellation token source for test control.
    /// </summary>
    protected CancellationTokenSource TestCancellation { get; private set; } = new();

    protected LoadTestBase(ITestOutputHelper output)
    {
        _output = output;
        Config = LoadTestConfiguration.FromEnvironment();
        Features = Config.Features;
        Metrics = new MetricsAggregator(Config.MetricsSamplingWindow);
        Reporter = new LoadTestReporter(output);
    }

    /// <summary>
    /// Extended timeout for long-running load tests.
    /// </summary>
    protected override TimeSpan DefaultTimeout => TimeSpan.FromMinutes(30);

    protected override void ConfigureAdditionalServices(Microsoft.Extensions.DependencyInjection.IServiceCollection services)
    {
        // Base implementation - subclasses can override
    }

    protected override async Task OnInitializeAsync()
    {
        await base.OnInitializeAsync();
        TestCancellation = new CancellationTokenSource();
    }

    protected override async Task OnDisposeAsync()
    {
        await TestCancellation.CancelAsync();

        if (_periodicReportingTask != null)
        {
            try
            {
                await _periodicReportingTask.WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (OperationCanceledException) { }
            catch (TimeoutException) { }
        }

        TestCancellation.Dispose();

        if (_sqlContainer != null)
        {
            await _sqlContainer.DisposeAsync();
            _sqlContainer = null;
            SqlConnectionString = null;
        }

        await base.OnDisposeAsync();
    }

    /// <summary>
    /// Starts periodic metrics reporting to console.
    /// </summary>
    protected void StartPeriodicReporting(TimeSpan? interval = null)
    {
        var reportInterval = interval ?? Config.MetricsReportingInterval;

        _periodicReportingTask = Task.Run(async () =>
        {
            try
            {
                while (!TestCancellation.IsCancellationRequested)
                {
                    await Task.Delay(reportInterval, TestCancellation.Token);
                    var snapshot = Metrics.GetSnapshot();
                    Reporter.ReportProgress(snapshot);
                    Reporter.RecordSnapshot(snapshot);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when test completes
            }
        }, TestCancellation.Token);
    }

    /// <summary>
    /// Performs warmup by publishing and consuming a small number of messages.
    /// </summary>
    protected async Task WarmupAsync<THandler>(IEventPublisher publisher, int count = 0)
        where THandler : class
    {
        count = count > 0 ? count : Config.WarmupMessageCount;

        Reporter.WriteLine($"Warmup: Publishing {count} messages...");

        for (int i = 0; i < count; i++)
        {
            await publisher.PublishAsync(new LoadTestEvent { Sequence = i, IsWarmup = true }, TestCancellation.Token);
        }

        await LoadTestEventHandler.WaitForCountAsync(count, TimeSpan.FromSeconds(60));

        Reporter.WriteLine($"Warmup: Completed, resetting metrics.");

        // Reset metrics and handlers after warmup
        Metrics.Reset();
        LoadTestEventHandler.Reset();
    }

    /// <summary>
    /// Waits for all published messages to be consumed with timeout.
    /// </summary>
    protected async Task WaitForConsumptionAsync(long expectedCount, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (Metrics.GetSnapshot().TotalConsumed < expectedCount && sw.Elapsed < timeout)
        {
            if (TestCancellation.IsCancellationRequested)
                break;

            await Task.Delay(100, TestCancellation.Token);
        }
    }

    /// <summary>
    /// Waits for handler to reach expected count.
    /// </summary>
    protected async Task WaitForHandlerCountAsync(long expectedCount, TimeSpan timeout)
    {
        await LoadTestEventHandler.WaitForCountAsync(expectedCount, timeout);
    }

    /// <summary>
    /// Asserts that message loss is within acceptable threshold.
    /// </summary>
    protected void AssertNoMessageLoss(double tolerancePercent = 0.0)
    {
        var metrics = Metrics.GetSnapshot();
        Assert.True(
            metrics.MessageLossPercentage <= tolerancePercent,
            $"Message loss {metrics.MessageLossPercentage:N4}% exceeds tolerance {tolerancePercent}%. " +
            $"Published: {metrics.TotalPublished}, Consumed: {metrics.TotalConsumed}");
    }

    /// <summary>
    /// Asserts that latency P99 is below threshold.
    /// </summary>
    protected void AssertLatencyP99(TimeSpan threshold)
    {
        var metrics = Metrics.GetSnapshot();
        Assert.True(
            metrics.LatencyStatistics.P99 <= threshold,
            $"P99 latency {metrics.LatencyStatistics.P99.TotalMilliseconds}ms exceeds threshold {threshold.TotalMilliseconds}ms");
    }

    /// <summary>
    /// Publishes messages at a controlled rate.
    /// </summary>
    protected async Task PublishAtRateAsync(
        IEventPublisher publisher,
        int targetRps,
        TimeSpan duration,
        Func<long, LoadTestEvent>? messageFactory = null)
    {
        var stopwatch = Stopwatch.StartNew();
        long sequence = 0;

        // Calculate batch parameters for efficient publishing
        var batchSize = Math.Max(1, Math.Min(100, targetRps / 10));
        var batchDelay = TimeSpan.FromMilliseconds(1000.0 / targetRps * batchSize);

        while (stopwatch.Elapsed < duration && !TestCancellation.IsCancellationRequested)
        {
            var batchTasks = new List<Task>(batchSize);

            for (int i = 0; i < batchSize && !TestCancellation.IsCancellationRequested; i++)
            {
                var seq = Interlocked.Increment(ref sequence);
                var message = messageFactory?.Invoke(seq) ?? new LoadTestEvent
                {
                    Sequence = seq,
                    PublishedAtTicks = Stopwatch.GetTimestamp()
                };

                batchTasks.Add(publisher.PublishAsync(message, TestCancellation.Token));
                Metrics.RecordPublished();
            }

            await Task.WhenAll(batchTasks);

            // Delay to maintain target rate
            await Task.Delay(batchDelay, TestCancellation.Token);
        }
    }

    /// <summary>
    /// Publishes messages at a precisely controlled rate using spin-wait for timing.
    /// Better for latency testing where consistent timing is important.
    /// </summary>
    protected async Task PublishAtPreciseRateAsync(
        IEventPublisher publisher,
        int targetRps,
        TimeSpan duration)
    {
        var stopwatch = Stopwatch.StartNew();
        long sequence = 0;
        var interval = TimeSpan.FromMilliseconds(1000.0 / targetRps);
        var nextPublishTime = stopwatch.Elapsed;

        while (stopwatch.Elapsed < duration && !TestCancellation.IsCancellationRequested)
        {
            if (stopwatch.Elapsed >= nextPublishTime)
            {
                var message = new LoadTestEvent
                {
                    Sequence = Interlocked.Increment(ref sequence),
                    PublishedAtTicks = Stopwatch.GetTimestamp()
                };

                await publisher.PublishAsync(message, TestCancellation.Token);
                Metrics.RecordPublished();

                nextPublishTime += interval;
            }
            else
            {
                // Precise timing using spin-wait for short delays
                var waitTime = nextPublishTime - stopwatch.Elapsed;
                if (waitTime > TimeSpan.FromMilliseconds(1))
                {
                    await Task.Delay(waitTime - TimeSpan.FromMilliseconds(0.5), TestCancellation.Token);
                }
                else if (waitTime > TimeSpan.Zero)
                {
                    Thread.SpinWait(100);
                }
            }
        }
    }

    #region SQL Server Container Management

    /// <summary>
    /// Ensures SQL Server container is available for persistence features.
    /// </summary>
    protected async Task EnsureSqlServerAsync()
    {
        if (_sqlContainer != null) return;

        Reporter.WriteLine("Starting SQL Server container for persistence features...");

        _sqlContainer = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("YourStrong@Passw0rd")
            .Build();

        await _sqlContainer.StartAsync();
        SqlConnectionString = _sqlContainer.GetConnectionString();

        Reporter.WriteLine($"SQL Server container started.");
    }

    #endregion

    #region Host Building with Features

    /// <summary>
    /// Builds a host configured with Redis Streams messaging and specified features.
    /// Automatically starts SQL Server container if persistence features are enabled.
    /// </summary>
    protected async Task<IHost> BuildHostWithFeatures<THandlerMarker>(FeatureConfiguration? features = null)
    {
        features ??= Features;

        // Start SQL Server if needed
        if (features.RequiresSqlServer)
        {
            await EnsureSqlServerAsync();
        }

        var testContext = TestContext;

        var hostBuilder = Host.CreateDefaultBuilder()
            .ConfigureServices((_, services) =>
            {
                services.AddLogging();

                // Register test context as singleton so it can be resolved by middleware
                services.AddSingleton(testContext);

                // Add middleware to set the test context before each message is processed
                services.AddSingleton<IConsumeMiddleware, LoadTestContextPropagationMiddleware>();

                var messagingBuilder = services.AddMessaging()
                    .UseRedisStreamsQueues(queues => queues
                        .WithConnection(opts =>
                        {
                            opts.UseConnectionString(RedisConnectionString);
                            opts.WithStreamPrefix(StreamPrefix);
                            opts.ConfigureConsumer(batchSize: 10, blockingTimeout: TimeSpan.FromMilliseconds(100));
                        })
                        .WithTopology(topology => topology
                            .WithServiceName("load-test-service")
                            .ScanAssemblyContaining<THandlerMarker>()));

                // Configure resilience features
                if (features.HasResilienceFeatures)
                {
                    messagingBuilder.UseResilience(resilience =>
                    {
                        if (features.RetryEnabled)
                        {
                            resilience.WithRetry(opts =>
                            {
                                opts.MaxRetryAttempts = features.RetryMaxAttempts;
                                opts.InitialDelay = features.RetryInitialDelay;
                            });
                        }

                        if (features.CircuitBreakerEnabled)
                        {
                            resilience.WithCircuitBreaker(opts =>
                            {
                                opts.FailureThreshold = features.CircuitBreakerFailureThreshold;
                                opts.DurationOfBreak = features.CircuitBreakerDurationOfBreak;
                            });
                        }

                        if (features.TimeoutEnabled)
                        {
                            resilience.WithTimeout(features.TimeoutDuration);
                        }
                    });
                }

                // Configure persistence features
                if (features.RequiresSqlServer)
                {
                    messagingBuilder.UsePersistence(persistence =>
                    {
                        if (features.OutboxEnabled)
                        {
                            persistence.WithOutbox(opts =>
                            {
                                opts.BatchSize = features.OutboxBatchSize;
                                opts.ProcessingInterval = features.OutboxProcessingInterval;
                                opts.AutoCreateSchema = true;
                            }).UseSqlServer(SqlConnectionString!);
                        }
                        else if (features.IdempotencyEnabled)
                        {
                            // Idempotency without outbox needs WithOutbox to register repositories
                            // but we'll use InMemoryMessageStoreProvider
                            persistence.WithOutbox(opts =>
                            {
                                opts.Enabled = false; // Disable outbox processing
                                opts.AutoCreateSchema = true;
                            });
                        }

                        if (features.IdempotencyEnabled)
                        {
                            persistence.WithIdempotency(opts =>
                            {
                                opts.RetentionPeriod = features.IdempotencyRetentionPeriod;
                            });
                        }
                    });

                    // Register InMemoryMessageStoreProvider if not using SQL Server outbox
                    if (!features.OutboxEnabled)
                    {
                        services.AddSingleton<IMessageStoreProvider>(sp =>
                            new InMemoryMessageStoreProvider());
                    }
                }
            });

        var host = hostBuilder.Build();
        await host.StartAsync();
        return host;
    }

    /// <summary>
    /// Creates a FeatureConfiguration with explicit settings.
    /// Use for test-specific feature combinations.
    /// </summary>
    protected static FeatureConfiguration CreateFeatures(
        bool outbox = false,
        int outboxBatchSize = 100,
        TimeSpan? outboxProcessingInterval = null,
        bool idempotency = false,
        TimeSpan? idempotencyRetention = null,
        bool retry = false,
        int retryMaxAttempts = 3,
        TimeSpan? retryInitialDelay = null,
        bool circuitBreaker = false,
        int circuitBreakerThreshold = 5,
        TimeSpan? circuitBreakerDuration = null,
        bool timeout = false,
        TimeSpan? timeoutDuration = null)
    {
        return new FeatureConfiguration
        {
            // Persistence
            OutboxEnabled = outbox,
            OutboxBatchSize = outboxBatchSize,
            OutboxProcessingInterval = outboxProcessingInterval ?? TimeSpan.FromSeconds(1),
            IdempotencyEnabled = idempotency,
            IdempotencyRetentionPeriod = idempotencyRetention ?? TimeSpan.FromDays(7),

            // Resilience
            RetryEnabled = retry,
            RetryMaxAttempts = retryMaxAttempts,
            RetryInitialDelay = retryInitialDelay ?? TimeSpan.FromMilliseconds(100),
            CircuitBreakerEnabled = circuitBreaker,
            CircuitBreakerFailureThreshold = circuitBreakerThreshold,
            CircuitBreakerDurationOfBreak = circuitBreakerDuration ?? TimeSpan.FromSeconds(30),
            TimeoutEnabled = timeout,
            TimeoutDuration = timeoutDuration ?? TimeSpan.FromSeconds(30)
        };
    }

    #endregion
}

/// <summary>
/// Middleware that propagates the test execution context to the consumer's async flow for load tests.
/// </summary>
internal sealed class LoadTestContextPropagationMiddleware(TestExecutionContext testContext) : IConsumeMiddleware
{
    public async Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
    {
        var previousContext = TestExecutionContextAccessor.Current;
        TestExecutionContextAccessor.Current = testContext;
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
