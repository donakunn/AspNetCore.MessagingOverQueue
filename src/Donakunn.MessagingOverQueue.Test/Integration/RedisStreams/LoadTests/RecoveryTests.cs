using System.Diagnostics;
using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection;
using Donakunn.MessagingOverQueue.Topology.DependencyInjection;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.TestMessages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Xunit.Abstractions;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests;

/// <summary>
/// Recovery tests: Consumer restart scenarios with pending messages.
/// Tests the consumer's ability to recover and process all pending messages.
/// </summary>
[Trait("Category", "LoadTest")]
public class RecoveryTests : LoadTestBase
{
    public RecoveryTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    [Trait("Duration", "Medium")]
    public async Task Consumer_Restart_With_10K_Pending_Messages()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        var pendingCount = Config.RecoveryTestPendingMessageCount;
        var streamKey = $"{StreamPrefix}:test-service.load-test-event";
        var consumerGroup = "test-service.load-test-event";

        Reporter.WriteLine($"Phase 1: Publishing {pendingCount} messages without consumer...");

        // Phase 1: Publish messages WITHOUT a consumer running
        var publisherHostBuilder = Host.CreateDefaultBuilder()
            .ConfigureServices((_, services) =>
            {
                services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Debug));

                services.AddRedisStreamsMessaging(options =>
                {
                    options.UseConnectionString(RedisConnectionString);
                    options.WithStreamPrefix(StreamPrefix);
                })
                .AddTopology(topology => topology
                    .WithServiceName("test-service")
                    .ScanAssemblyContaining<LoadTestEventHandler>());
                // Note: NOT adding consumer hosted service - just publisher
            });

        using (var publisherHost = publisherHostBuilder.Build())
        {
            await publisherHost.StartAsync();

            var publisher = publisherHost.Services.GetRequiredService<IEventPublisher>();

            // Ensure consumer group exists so messages go to pending list
            var db = GetRedisDatabase();
            try
            {
                await db.StreamCreateConsumerGroupAsync(streamKey, consumerGroup, "0", createStream: true);
            }
            catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
            {
                // Group already exists
            }

            // Publish all messages
            var publishTasks = new List<Task>(pendingCount);
            for (int i = 0; i < pendingCount; i++)
            {
                var msg = new LoadTestEvent
                {
                    Sequence = i,
                    PublishedAtTicks = Stopwatch.GetTimestamp()
                };
                publishTasks.Add(publisher.PublishAsync(msg, TestCancellation.Token));
                Metrics.RecordPublished();

                // Batch publish for efficiency
                if (publishTasks.Count >= 100)
                {
                    await Task.WhenAll(publishTasks);
                    publishTasks.Clear();
                }
            }

            if (publishTasks.Count > 0)
            {
                await Task.WhenAll(publishTasks);
            }

            await publisherHost.StopAsync();
        }

        // Verify messages are in the stream
        var streamLength = await GetStreamLengthAsync(streamKey);
        Reporter.WriteLine($"Stream contains {streamLength} messages");
        Assert.True(streamLength >= pendingCount * 0.99, $"Expected at least {pendingCount * 0.99} messages in stream, got {streamLength}");

        // Phase 2: Start consumer and measure recovery
        Reporter.WriteLine("Phase 2: Starting consumer for recovery...");

        Metrics.Start();
        var recoveryStart = Stopwatch.StartNew();

        using var consumerHost = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 100)
                   .ConfigureClaiming(claimIdleTime: TimeSpan.FromSeconds(2)));

        // Wait for all messages to be consumed
        await LoadTestEventHandler.WaitForCountAsync(pendingCount, Config.RecoveryTestTimeout);

        recoveryStart.Stop();
        Metrics.Stop();

        // Assert
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Recovery Test");

        var recoveryRate = pendingCount / recoveryStart.Elapsed.TotalSeconds;
        Reporter.WriteLine($"Recovery Time: {recoveryStart.Elapsed:mm\\:ss\\.fff}");
        Reporter.WriteLine($"Recovery Rate: {recoveryRate:N1} msg/sec");

        Assert.Equal(pendingCount, LoadTestEventHandler.HandleCount);
        Assert.True(
            recoveryStart.Elapsed < Config.RecoveryTestTimeout,
            $"Recovery took {recoveryStart.Elapsed}, expected under {Config.RecoveryTestTimeout}");
    }

    [Fact]
    [Trait("Duration", "Medium")]
    public async Task Consumer_Crash_Recovery_Preserves_Messages()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        const int messagesToPublish = 1000;
        const int messagesToProcessBeforeCrash = 500;

        Reporter.WriteLine("Phase 1: Processing messages then simulating crash...");

        // Phase 1: Start consumer and process some messages
        using (var host1 = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureClaiming(TimeSpan.FromSeconds(5))))
        {
            var publisher = host1.Services.GetRequiredService<IEventPublisher>();

            // Publish all messages
            for (int i = 0; i < messagesToPublish; i++)
            {
                await publisher.PublishAsync(new LoadTestEvent { Sequence = i }, TestCancellation.Token);
                Metrics.RecordPublished();
            }

            // Wait for partial processing
            await LoadTestEventHandler.WaitForCountAsync(messagesToProcessBeforeCrash, TimeSpan.FromSeconds(30));
        }
        // Host disposed = simulated crash

        var processedBeforeCrash = LoadTestEventHandler.HandleCount;
        Reporter.WriteLine($"Processed before crash: {processedBeforeCrash}");

        // Phase 2: Start new consumer and verify remaining messages processed
        Reporter.WriteLine("Phase 2: Recovery with new consumer...");

        // Don't reset handler - we want to continue counting
        using var host2 = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureClaiming(TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(500)));

        // Wait for remaining messages (some messages may be processed twice due to crash)
        await LoadTestEventHandler.WaitForCountAsync(messagesToPublish, TimeSpan.FromMinutes(2));

        // Assert
        Reporter.WriteLine($"Total processed: {LoadTestEventHandler.HandleCount}");

        // Some messages may be processed twice due to crash, but none should be lost
        Assert.True(
            LoadTestEventHandler.HandleCount >= messagesToPublish,
            $"Expected at least {messagesToPublish} messages processed, got {LoadTestEventHandler.HandleCount}");
    }

    [Fact]
    [Trait("Duration", "Medium")]
    public async Task Consumer_Recovery_After_Network_Partition_Simulation()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        const int messagesToPublish = 500;

        using var host = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureConsumer(batchSize: 50));

        var publisher = host.Services.GetRequiredService<IEventPublisher>();

        await WarmupAsync<LoadTestEventHandler>(publisher, 50);

        Reporter.WriteLine($"Publishing {messagesToPublish} messages with simulated pauses...");

        // Act - Publish in bursts with pauses (simulating network issues)
        Metrics.Start();

        for (int batch = 0; batch < 5; batch++)
        {
            var batchSize = messagesToPublish / 5;

            for (int i = 0; i < batchSize; i++)
            {
                var seq = batch * batchSize + i;
                await publisher.PublishAsync(new LoadTestEvent { Sequence = seq }, TestCancellation.Token);
                Metrics.RecordPublished();
            }

            Reporter.WriteLine($"Batch {batch + 1}/5 complete, pausing...");
            await Task.Delay(TimeSpan.FromSeconds(2), TestCancellation.Token);
        }

        await WaitForConsumptionAsync(messagesToPublish, TimeSpan.FromMinutes(2));

        Metrics.Stop();

        // Assert
        var finalMetrics = Metrics.GetSnapshot();
        Reporter.ReportFinal(finalMetrics, "Network Partition Recovery Test");

        AssertNoMessageLoss();
    }

    [Fact]
    [Trait("Duration", "Short")]
    public async Task Consumer_Graceful_Shutdown_No_Message_Loss()
    {
        // Arrange
        LoadTestEventHandler.Reset();
        LoadTestEventHandler.SetMetricsCollector(Metrics);

        const int messagesToPublish = 200;

        Reporter.WriteLine("Testing graceful shutdown preserves messages...");

        long processedInFirstPhase;

        // Phase 1: Publish and process some messages, then gracefully stop
        using (var host1 = await BuildHost<LoadTestEventHandler>())
        {
            var publisher = host1.Services.GetRequiredService<IEventPublisher>();

            for (int i = 0; i < messagesToPublish; i++)
            {
                await publisher.PublishAsync(new LoadTestEvent { Sequence = i }, TestCancellation.Token);
                Metrics.RecordPublished();
            }

            // Wait for some processing
            await Task.Delay(500, TestCancellation.Token);
            processedInFirstPhase = LoadTestEventHandler.HandleCount;

            // Graceful shutdown (host.StopAsync is called by Dispose)
        }

        Reporter.WriteLine($"Processed in first phase: {processedInFirstPhase}");

        // Phase 2: Restart and verify all messages eventually processed
        using var host2 = await BuildHost<LoadTestEventHandler>(options =>
            options.ConfigureClaiming(TimeSpan.FromSeconds(2)));

        await LoadTestEventHandler.WaitForCountAsync(messagesToPublish, TimeSpan.FromMinutes(1));

        // Assert
        Assert.True(
            LoadTestEventHandler.HandleCount >= messagesToPublish,
            $"Expected at least {messagesToPublish} messages, got {LoadTestEventHandler.HandleCount}");
    }
}
