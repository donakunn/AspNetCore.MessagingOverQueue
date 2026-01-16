using System.Diagnostics;
using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using MessagingOverQueue.Test.Integration.RedisStreams.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Metrics;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.TestMessages;
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
}
