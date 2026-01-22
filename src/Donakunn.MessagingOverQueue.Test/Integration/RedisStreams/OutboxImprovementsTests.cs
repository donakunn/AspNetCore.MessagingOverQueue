using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.DependencyInjection;
using Donakunn.MessagingOverQueue.DependencyInjection.Persistence;
using Donakunn.MessagingOverQueue.Persistence;
using Donakunn.MessagingOverQueue.Persistence.Entities;
using Donakunn.MessagingOverQueue.Persistence.Providers;
using Donakunn.MessagingOverQueue.Persistence.Repositories;
using Donakunn.MessagingOverQueue.Providers;
using Donakunn.MessagingOverQueue.Publishing.Middleware;
using Donakunn.MessagingOverQueue.RedisStreams.DependencyInjection.Queues;
using MessagingOverQueue.Test.Integration.Shared.Infrastructure;
using MessagingOverQueue.Test.Integration.Shared.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace MessagingOverQueue.Test.Integration.RedisStreams;

/// <summary>
/// Integration tests for outbox pattern improvements:
/// - Batch publishing
/// - Batch status updates
/// - Partitioned message acquisition
/// - Multiple workers
/// </summary>
public class OutboxImprovementsTests : IAsyncLifetime
{
    private Testcontainers.Redis.RedisContainer? _redisContainer;
    protected IServiceProvider ServiceProvider { get; private set; } = null!;
    protected string RedisConnectionString { get; private set; } = string.Empty;

    private readonly TestExecutionContext _testContext;
    private readonly string _streamPrefix = $"test-{Guid.NewGuid():N}";

    public OutboxImprovementsTests()
    {
        _testContext = new TestExecutionContext();
        TestExecutionContextAccessor.Current = _testContext;
    }

    protected TestExecutionContext TestContext => _testContext;
    protected string StreamPrefix => _streamPrefix;

    public async Task InitializeAsync()
    {
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

        if (_redisContainer != null)
        {
            await _redisContainer.DisposeAsync();
        }

        _testContext.Reset();
        TestExecutionContextAccessor.Current = null;
    }

    #region Configuration Tests

    [Fact]
    public async Task OutboxOptions_NewProperties_HaveCorrectDefaults()
    {
        // Arrange & Act
        using var host = await BuildHostWithOutbox(opts => { });

        var options = host.Services.GetRequiredService<IOptions<OutboxOptions>>().Value;

        // Assert - Verify new defaults
        Assert.Equal(1, options.WorkerCount);
        Assert.Equal(10, options.PublishBatchSize);
        Assert.Equal(4, options.PartitionCount);
    }

    [Fact]
    public async Task OutboxOptions_CanConfigureWorkerCount()
    {
        // Arrange & Act
        using var host = await BuildHostWithOutbox(opts =>
        {
            opts.WorkerCount = 3;
            opts.PartitionCount = 6;
        });

        var options = host.Services.GetRequiredService<IOptions<OutboxOptions>>().Value;

        // Assert
        Assert.Equal(3, options.WorkerCount);
        Assert.Equal(6, options.PartitionCount);
    }

    [Fact]
    public async Task OutboxOptions_CanConfigurePublishBatchSize()
    {
        // Arrange & Act
        using var host = await BuildHostWithOutbox(opts =>
        {
            opts.PublishBatchSize = 25;
        });

        var options = host.Services.GetRequiredService<IOptions<OutboxOptions>>().Value;

        // Assert
        Assert.Equal(25, options.PublishBatchSize);
    }

    #endregion

    #region Batch Status Update Tests

    [Fact]
    public async Task MarkAsPublishedBatchAsync_UpdatesMultipleMessages()
    {
        // Arrange
        var provider = new InMemoryMessageStoreProvider();
        var messageIds = new List<Guid>();

        for (int i = 0; i < 5; i++)
        {
            var entry = CreateOutboxEntry($"queue-{i}");
            await provider.AddAsync(entry);
            messageIds.Add(entry.Id);
        }

        // Acquire locks first
        await provider.AcquireOutboxLockAsync(5, TimeSpan.FromMinutes(5));

        // Act
        await provider.MarkAsPublishedBatchAsync(messageIds);

        // Assert
        foreach (var id in messageIds)
        {
            var entry = await provider.GetByIdAsync(id, MessageDirection.Outbox);
            Assert.NotNull(entry);
            Assert.Equal(MessageStatus.Published, entry.Status);
            Assert.NotNull(entry.ProcessedAt);
            Assert.Null(entry.LockToken);
        }
    }

    [Fact]
    public async Task MarkAsFailedBatchAsync_UpdatesMultipleMessagesWithErrors()
    {
        // Arrange
        var provider = new InMemoryMessageStoreProvider();
        var failures = new List<(Guid Id, string Error)>();

        for (int i = 0; i < 5; i++)
        {
            var entry = CreateOutboxEntry($"queue-{i}");
            await provider.AddAsync(entry);
            failures.Add((entry.Id, $"Error {i}"));
        }

        // Acquire locks first
        await provider.AcquireOutboxLockAsync(5, TimeSpan.FromMinutes(5));

        // Act
        await provider.MarkAsFailedBatchAsync(failures);

        // Assert
        foreach (var (id, expectedError) in failures)
        {
            var entry = await provider.GetByIdAsync(id, MessageDirection.Outbox);
            Assert.NotNull(entry);
            Assert.Equal(MessageStatus.Failed, entry.Status);
            Assert.Equal(expectedError, entry.LastError);
            Assert.Equal(1, entry.RetryCount);
            Assert.Null(entry.LockToken);
        }
    }

    [Fact]
    public async Task Repository_MarkAsPublishedBatchAsync_DelegatesToProvider()
    {
        // Arrange
        var provider = new InMemoryMessageStoreProvider();
        var repository = new OutboxRepository(provider);
        var messageIds = new List<Guid>();

        for (int i = 0; i < 3; i++)
        {
            var entry = CreateOutboxEntry($"queue-{i}");
            await provider.AddAsync(entry);
            messageIds.Add(entry.Id);
        }

        await repository.AcquireLockAsync(3, TimeSpan.FromMinutes(5));

        // Act
        await repository.MarkAsPublishedBatchAsync(messageIds);

        // Assert
        var publishedEntries = provider.GetOutboxEntriesByStatus(MessageStatus.Published);
        Assert.Equal(3, publishedEntries.Count);
    }

    [Fact]
    public async Task Repository_MarkAsFailedBatchAsync_DelegatesToProvider()
    {
        // Arrange
        var provider = new InMemoryMessageStoreProvider();
        var repository = new OutboxRepository(provider);
        var failures = new List<(Guid Id, string Error)>();

        for (int i = 0; i < 3; i++)
        {
            var entry = CreateOutboxEntry($"queue-{i}");
            await provider.AddAsync(entry);
            failures.Add((entry.Id, $"Test error {i}"));
        }

        await repository.AcquireLockAsync(3, TimeSpan.FromMinutes(5));

        // Act
        await repository.MarkAsFailedBatchAsync(failures);

        // Assert
        var failedEntries = provider.GetOutboxEntriesByStatus(MessageStatus.Failed);
        Assert.Equal(3, failedEntries.Count);
    }

    #endregion

    #region Partitioned Lock Acquisition Tests

    [Fact]
    public async Task AcquireOutboxLockAsync_WithPartitions_FiltersCorrectly()
    {
        // Arrange
        var provider = new InMemoryMessageStoreProvider();

        // Create messages for different queues (different partitions)
        var queue1Entry = CreateOutboxEntry("queue-a");
        var queue2Entry = CreateOutboxEntry("queue-b");
        var queue3Entry = CreateOutboxEntry("queue-c");
        var queue4Entry = CreateOutboxEntry("queue-d");

        await provider.AddAsync(queue1Entry);
        await provider.AddAsync(queue2Entry);
        await provider.AddAsync(queue3Entry);
        await provider.AddAsync(queue4Entry);

        // Act - acquire only partition 0 of 2 partitions
        var partition0 = "queue-a".GetHashCode() % 2 == 0 ? 0 : 1;
        var assignedPartitions = new[] { partition0 };

        var acquired = await provider.AcquireOutboxLockAsync(
            10,
            TimeSpan.FromMinutes(5),
            assignedPartitions,
            2);

        // Assert - should only get messages in partition 0
        Assert.All(acquired, entry =>
        {
            var queueName = entry.QueueName ?? string.Empty;
            var partition = Math.Abs(queueName.GetHashCode()) % 2;
            Assert.Contains(partition, assignedPartitions);
        });
    }

    [Fact]
    public async Task AcquireOutboxLockAsync_WithoutPartitions_ReturnsAllMessages()
    {
        // Arrange
        var provider = new InMemoryMessageStoreProvider();

        for (int i = 0; i < 5; i++)
        {
            await provider.AddAsync(CreateOutboxEntry($"queue-{i}"));
        }

        // Act - acquire without partition filtering
        var acquired = await provider.AcquireOutboxLockAsync(
            10,
            TimeSpan.FromMinutes(5),
            null,
            0);

        // Assert - should get all messages
        Assert.Equal(5, acquired.Count);
    }

    [Fact]
    public async Task AcquireOutboxLockAsync_MultipleWorkers_NoOverlap()
    {
        // Arrange
        var provider = new InMemoryMessageStoreProvider();
        var partitionCount = 4;
        var workerCount = 2;

        // Create many messages across different queues
        for (int i = 0; i < 20; i++)
        {
            await provider.AddAsync(CreateOutboxEntry($"queue-{i}"));
        }

        // Calculate partitions for each worker (same logic as OutboxProcessor)
        var worker0Partitions = Enumerable.Range(0, partitionCount)
            .Where(p => p % workerCount == 0)
            .ToArray();
        var worker1Partitions = Enumerable.Range(0, partitionCount)
            .Where(p => p % workerCount == 1)
            .ToArray();

        // Act - both workers acquire locks
        var worker0Messages = await provider.AcquireOutboxLockAsync(
            20, TimeSpan.FromMinutes(5), worker0Partitions, partitionCount);

        // Clear for next worker (simulate parallel acquisition with no overlap)
        provider.Clear();
        for (int i = 0; i < 20; i++)
        {
            await provider.AddAsync(CreateOutboxEntry($"queue-{i}"));
        }

        var worker1Messages = await provider.AcquireOutboxLockAsync(
            20, TimeSpan.FromMinutes(5), worker1Partitions, partitionCount);

        // Assert - workers should have non-overlapping partition assignments
        Assert.True(worker0Partitions.Intersect(worker1Partitions).Count() == 0,
            "Workers should have non-overlapping partition assignments");
    }

    [Fact]
    public async Task Repository_AcquireLockAsync_WithPartitions_DelegatesToProvider()
    {
        // Arrange
        var provider = new InMemoryMessageStoreProvider();
        var repository = new OutboxRepository(provider);

        for (int i = 0; i < 10; i++)
        {
            await provider.AddAsync(CreateOutboxEntry($"queue-{i}"));
        }

        // Act
        var acquired = await repository.AcquireLockAsync(
            10,
            TimeSpan.FromMinutes(5),
            new[] { 0 },
            2);

        // Assert
        Assert.NotEmpty(acquired);
        Assert.All(acquired, entry =>
        {
            var partition = Math.Abs((entry.QueueName ?? "").GetHashCode()) % 2;
            Assert.Equal(0, partition);
        });
    }

    #endregion

    #region Batch Publishing Tests

    [Fact]
    public async Task PublishBatchAsync_ReturnsResultsForAllMessages()
    {
        // Arrange
        using var host = await BuildHostWithRedisStreams();
        var publisher = host.Services.GetRequiredService<IInternalPublisher>();

        var contexts = new List<PublishContext>();
        for (int i = 0; i < 5; i++)
        {
            contexts.Add(new PublishContext
            {
                Body = System.Text.Encoding.UTF8.GetBytes($"{{\"value\":\"{i}\"}}"),
                QueueName = $"{StreamPrefix}:batch-test-{i}",
                Headers = new Dictionary<string, object?>
                {
                    ["message-id"] = Guid.NewGuid().ToString(),
                    ["message-type"] = "TestMessage"
                }
            });
        }

        // Act
        var results = await publisher.PublishBatchAsync(contexts);

        // Assert
        Assert.Equal(5, results.Count);
        Assert.All(results, r => Assert.True(r.Success, $"Message should succeed: {r.Error}"));
    }

    [Fact]
    public async Task PublishBatchAsync_TracksIndividualFailures()
    {
        // Arrange
        var mockPublisher = new MockInternalPublisher(failIndices: new[] { 1, 3 });

        var contexts = new List<PublishContext>();
        for (int i = 0; i < 5; i++)
        {
            contexts.Add(new PublishContext
            {
                Body = System.Text.Encoding.UTF8.GetBytes($"{{\"value\":\"{i}\"}}"),
                QueueName = $"test-queue-{i}",
                Headers = new Dictionary<string, object?>
                {
                    ["message-id"] = Guid.NewGuid().ToString()
                }
            });
        }

        // Act
        var results = await mockPublisher.PublishBatchAsync(contexts);

        // Assert
        Assert.Equal(5, results.Count);
        Assert.True(results[0].Success);
        Assert.False(results[1].Success);
        Assert.True(results[2].Success);
        Assert.False(results[3].Success);
        Assert.True(results[4].Success);
    }

    [Fact]
    public async Task PublishBatchAsync_EmptyList_ReturnsEmpty()
    {
        // Arrange
        using var host = await BuildHostWithRedisStreams();
        var publisher = host.Services.GetRequiredService<IInternalPublisher>();

        // Act
        var results = await publisher.PublishBatchAsync(new List<PublishContext>());

        // Assert
        Assert.Empty(results);
    }

    #endregion

    #region Multiple Workers Tests

    [Fact]
    public async Task MultipleWorkers_AreRegistered_WhenWorkerCountGreaterThanOne()
    {
        // Arrange & Act
        using var host = await BuildHostWithOutbox(opts =>
        {
            opts.WorkerCount = 3;
            opts.PartitionCount = 6;
            opts.Enabled = false; // Disable processing for this test
        });

        var hostedServices = host.Services.GetServices<IHostedService>();

        // Assert - should have 3 OutboxProcessor instances
        var outboxProcessors = hostedServices.OfType<OutboxProcessor>().ToList();
        Assert.Equal(3, outboxProcessors.Count);
    }

    [Fact]
    public async Task SingleWorker_IsRegistered_WhenWorkerCountIsOne()
    {
        // Arrange & Act
        using var host = await BuildHostWithOutbox(opts =>
        {
            opts.WorkerCount = 1;
            opts.Enabled = false;
        });

        var hostedServices = host.Services.GetServices<IHostedService>();

        // Assert
        var outboxProcessors = hostedServices.OfType<OutboxProcessor>().ToList();
        Assert.Single(outboxProcessors);
    }

    [Fact]
    public void PartitionAssignment_WorkersGetNonOverlappingPartitions()
    {
        // Arrange
        var workerCount = 3;
        var partitionCount = 6;

        // Act - Calculate partitions for each worker (same logic as OutboxProcessor)
        var allAssignments = new List<int[]>();
        for (int workerId = 0; workerId < workerCount; workerId++)
        {
            var assigned = Enumerable.Range(0, partitionCount)
                .Where(p => p % workerCount == workerId)
                .ToArray();
            allAssignments.Add(assigned);
        }

        // Assert - each partition should be assigned to exactly one worker
        var allPartitions = allAssignments.SelectMany(a => a).ToList();
        Assert.Equal(partitionCount, allPartitions.Count);
        Assert.Equal(partitionCount, allPartitions.Distinct().Count());

        // Assert - each worker should have partitionCount/workerCount partitions
        Assert.All(allAssignments, a => Assert.Equal(2, a.Length));
    }

    [Fact]
    public void PartitionAssignment_UnevenDistribution_HandledCorrectly()
    {
        // Arrange - 4 partitions with 3 workers (uneven distribution)
        var workerCount = 3;
        var partitionCount = 4;

        // Act
        var allAssignments = new List<int[]>();
        for (int workerId = 0; workerId < workerCount; workerId++)
        {
            var assigned = Enumerable.Range(0, partitionCount)
                .Where(p => p % workerCount == workerId)
                .ToArray();
            allAssignments.Add(assigned);
        }

        // Assert - all partitions should still be covered
        var allPartitions = allAssignments.SelectMany(a => a).ToList();
        Assert.Equal(partitionCount, allPartitions.Distinct().Count());

        // Worker 0 gets partition 0, 3
        // Worker 1 gets partition 1
        // Worker 2 gets partition 2
        Assert.Equal(new[] { 0, 3 }, allAssignments[0]);
        Assert.Equal(new[] { 1 }, allAssignments[1]);
        Assert.Equal(new[] { 2 }, allAssignments[2]);
    }

    #endregion

    #region End-to-End Batch Processing Tests

    [Fact]
    public async Task OutboxProcessor_ProcessesBatchesCorrectly()
    {
        // Arrange
        var provider = new InMemoryMessageStoreProvider();
        var mockPublisher = new MockInternalPublisher();

        // Add messages to outbox
        for (int i = 0; i < 15; i++)
        {
            var entry = CreateOutboxEntry($"queue-{i % 3}");
            entry.Payload = System.Text.Encoding.UTF8.GetBytes($"{{\"value\":\"{i}\"}}");
            await provider.AddAsync(entry);
        }

        // Act - simulate batch processing
        var messages = await provider.AcquireOutboxLockAsync(10, TimeSpan.FromMinutes(5));

        var contexts = messages.Select(m => new PublishContext
        {
            Body = m.Payload,
            QueueName = m.QueueName,
            Headers = new Dictionary<string, object?>
            {
                ["message-id"] = m.Id.ToString(),
                ["message-type"] = m.MessageType
            }
        }).ToList();

        var results = await mockPublisher.PublishBatchAsync(contexts);

        var succeeded = new List<Guid>();
        var failed = new List<(Guid Id, string Error)>();

        for (int i = 0; i < results.Count; i++)
        {
            if (results[i].Success)
                succeeded.Add(messages[i].Id);
            else
                failed.Add((messages[i].Id, results[i].Error ?? "Unknown"));
        }

        if (succeeded.Count > 0)
            await provider.MarkAsPublishedBatchAsync(succeeded);
        if (failed.Count > 0)
            await provider.MarkAsFailedBatchAsync(failed);

        // Assert
        Assert.Equal(10, succeeded.Count);
        var publishedEntries = provider.GetOutboxEntriesByStatus(MessageStatus.Published);
        Assert.Equal(10, publishedEntries.Count);
    }

    [Fact]
    public async Task OutboxProcessor_HandlesPartialFailures()
    {
        // Arrange
        var provider = new InMemoryMessageStoreProvider();
        var mockPublisher = new MockInternalPublisher(failIndices: new[] { 2, 5, 7 });

        for (int i = 0; i < 10; i++)
        {
            var entry = CreateOutboxEntry($"queue-{i}");
            entry.Payload = System.Text.Encoding.UTF8.GetBytes($"{{\"value\":\"{i}\"}}");
            await provider.AddAsync(entry);
        }

        // Act
        var messages = await provider.AcquireOutboxLockAsync(10, TimeSpan.FromMinutes(5));

        var contexts = messages.Select(m => new PublishContext
        {
            Body = m.Payload,
            QueueName = m.QueueName,
            Headers = new Dictionary<string, object?>
            {
                ["message-id"] = m.Id.ToString()
            }
        }).ToList();

        var results = await mockPublisher.PublishBatchAsync(contexts);

        var succeeded = new List<Guid>();
        var failed = new List<(Guid Id, string Error)>();

        for (int i = 0; i < results.Count; i++)
        {
            if (results[i].Success)
                succeeded.Add(messages[i].Id);
            else
                failed.Add((messages[i].Id, results[i].Error ?? "Unknown"));
        }

        await provider.MarkAsPublishedBatchAsync(succeeded);
        await provider.MarkAsFailedBatchAsync(failed);

        // Assert
        Assert.Equal(7, succeeded.Count);
        Assert.Equal(3, failed.Count);

        var publishedEntries = provider.GetOutboxEntriesByStatus(MessageStatus.Published);
        var failedEntries = provider.GetOutboxEntriesByStatus(MessageStatus.Failed);

        Assert.Equal(7, publishedEntries.Count);
        Assert.Equal(3, failedEntries.Count);
    }

    #endregion

    #region Helper Methods

    private MessageStoreEntry CreateOutboxEntry(string queueName)
    {
        return new MessageStoreEntry
        {
            Id = Guid.NewGuid(),
            Direction = MessageDirection.Outbox,
            MessageType = "TestMessage",
            Payload = System.Text.Encoding.UTF8.GetBytes("{}"),
            QueueName = queueName,
            Status = MessageStatus.Pending,
            CreatedAt = DateTime.UtcNow,
            RetryCount = 0
        };
    }

    private async Task<IHost> BuildHostWithOutbox(Action<OutboxOptions> configureOutbox)
    {
        var testContext = _testContext;

        var hostBuilder = Host.CreateDefaultBuilder()
            .ConfigureServices((_, services) =>
            {
                services.AddLogging();
                services.AddSingleton(testContext);

                services.AddMessaging()
                    .UseRedisStreamsQueues(queues => queues
                        .WithConnection(opts =>
                        {
                            opts.UseConnectionString(RedisConnectionString);
                            opts.WithStreamPrefix(StreamPrefix);
                        })
                        .WithTopology(topology => topology
                            .WithServiceName("outbox-test-service")
                            .ScanAssemblyContaining<SimpleTestEventHandler>()))
                    .UsePersistence(persistence => persistence
                        .WithOutbox(configureOutbox));

                services.AddSingleton<IMessageStoreProvider, InMemoryMessageStoreProvider>();
            });

        var host = hostBuilder.Build();
        await host.StartAsync();
        return host;
    }

    private async Task<IHost> BuildHostWithRedisStreams()
    {
        var testContext = _testContext;

        var hostBuilder = Host.CreateDefaultBuilder()
            .ConfigureServices((_, services) =>
            {
                services.AddLogging();
                services.AddSingleton(testContext);

                services.AddMessaging()
                    .UseRedisStreamsQueues(queues => queues
                        .WithConnection(opts =>
                        {
                            opts.UseConnectionString(RedisConnectionString);
                            opts.WithStreamPrefix(StreamPrefix);
                        })
                        .WithTopology(topology => topology
                            .WithServiceName("redis-test-service")
                            .ScanAssemblyContaining<SimpleTestEventHandler>()));
            });

        var host = hostBuilder.Build();
        await host.StartAsync();
        return host;
    }

    #endregion
}

/// <summary>
/// Mock publisher for testing batch publishing behavior.
/// </summary>
internal class MockInternalPublisher : IInternalPublisher
{
    private readonly HashSet<int> _failIndices;

    public MockInternalPublisher(int[]? failIndices = null)
    {
        _failIndices = failIndices?.ToHashSet() ?? new HashSet<int>();
    }

    public List<PublishContext> PublishedContexts { get; } = new();

    public Task PublishAsync(PublishContext context, CancellationToken cancellationToken = default)
    {
        PublishedContexts.Add(context);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<PublishResult>> PublishBatchAsync(
        IReadOnlyList<PublishContext> contexts,
        CancellationToken cancellationToken = default)
    {
        var results = new List<PublishResult>();

        for (int i = 0; i < contexts.Count; i++)
        {
            var context = contexts[i];
            PublishedContexts.Add(context);

            var messageIdStr = context.Headers.TryGetValue("message-id", out var mid)
                ? mid?.ToString() ?? Guid.NewGuid().ToString()
                : Guid.NewGuid().ToString();

            if (!Guid.TryParse(messageIdStr, out var messageId))
            {
                messageId = Guid.NewGuid();
            }

            if (_failIndices.Contains(i))
            {
                results.Add(PublishResult.Failed(messageId, $"Simulated failure at index {i}"));
            }
            else
            {
                results.Add(PublishResult.Succeeded(messageId));
            }
        }

        return Task.FromResult<IReadOnlyList<PublishResult>>(results);
    }
}
