using Donakunn.MessagingOverQueue.Configuration.Options;
using Donakunn.MessagingOverQueue.Persistence.Entities;
using Donakunn.MessagingOverQueue.Persistence.Providers;
using Donakunn.MessagingOverQueue.Persistence.Repositories;
using Donakunn.MessagingOverQueue.Providers;
using Donakunn.MessagingOverQueue.Publishing.Middleware;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace MessagingOverQueue.Test.Unit.Persistence;

/// <summary>
/// Unit tests for OutboxProcessor batch processing and partitioning logic.
/// </summary>
public class OutboxProcessorTests
{
    #region Partition Assignment Tests

    [Theory]
    [InlineData(1, 4, 0, new[] { 0, 1, 2, 3 })] // Single worker gets all partitions
    [InlineData(2, 4, 0, new[] { 0, 2 })]        // Worker 0 of 2
    [InlineData(2, 4, 1, new[] { 1, 3 })]        // Worker 1 of 2
    [InlineData(3, 6, 0, new[] { 0, 3 })]        // Worker 0 of 3
    [InlineData(3, 6, 1, new[] { 1, 4 })]        // Worker 1 of 3
    [InlineData(3, 6, 2, new[] { 2, 5 })]        // Worker 2 of 3
    [InlineData(4, 4, 0, new[] { 0 })]           // 4 workers, 4 partitions - each gets 1
    [InlineData(4, 4, 3, new[] { 3 })]
    public void PartitionAssignment_CalculatesCorrectly(
        int workerCount, int partitionCount, int workerId, int[] expectedPartitions)
    {
        // Act - same calculation as OutboxProcessor
        var assignedPartitions = Enumerable.Range(0, partitionCount)
            .Where(p => p % workerCount == workerId)
            .ToArray();

        // Assert
        Assert.Equal(expectedPartitions, assignedPartitions);
    }

    [Fact]
    public void PartitionAssignment_AllPartitionsCovered_WithMultipleWorkers()
    {
        // Arrange
        var workerCount = 3;
        var partitionCount = 9;

        // Act
        var allAssignedPartitions = new HashSet<int>();
        for (int workerId = 0; workerId < workerCount; workerId++)
        {
            var assigned = Enumerable.Range(0, partitionCount)
                .Where(p => p % workerCount == workerId);
            foreach (var p in assigned)
            {
                allAssignedPartitions.Add(p);
            }
        }

        // Assert - all partitions should be covered
        Assert.Equal(partitionCount, allAssignedPartitions.Count);
        for (int i = 0; i < partitionCount; i++)
        {
            Assert.Contains(i, allAssignedPartitions);
        }
    }

    [Fact]
    public void PartitionAssignment_NoOverlap_BetweenWorkers()
    {
        // Arrange
        var workerCount = 4;
        var partitionCount = 12;

        // Act
        var workerAssignments = new List<HashSet<int>>();
        for (int workerId = 0; workerId < workerCount; workerId++)
        {
            var assigned = Enumerable.Range(0, partitionCount)
                .Where(p => p % workerCount == workerId)
                .ToHashSet();
            workerAssignments.Add(assigned);
        }

        // Assert - no overlap between any two workers
        for (int i = 0; i < workerCount; i++)
        {
            for (int j = i + 1; j < workerCount; j++)
            {
                var overlap = workerAssignments[i].Intersect(workerAssignments[j]);
                Assert.Empty(overlap);
            }
        }
    }

    [Theory]
    [InlineData(3, 4)]  // More workers than partitions
    [InlineData(5, 4)]
    [InlineData(10, 4)]
    public void PartitionAssignment_MoreWorkersThanPartitions_SomeWorkersGetNone(
        int workerCount, int partitionCount)
    {
        // Act
        var totalAssignments = 0;
        var workersWithPartitions = 0;

        for (int workerId = 0; workerId < workerCount; workerId++)
        {
            var assigned = Enumerable.Range(0, partitionCount)
                .Where(p => p % workerCount == workerId)
                .ToArray();

            totalAssignments += assigned.Length;
            if (assigned.Length > 0)
                workersWithPartitions++;
        }

        // Assert - total assignments equals partition count
        Assert.Equal(partitionCount, totalAssignments);

        // Some workers may have no partitions when workerCount > partitionCount
        if (workerCount > partitionCount)
        {
            Assert.True(workersWithPartitions <= partitionCount);
        }
    }

    #endregion

    #region Batch Processing Logic Tests

    [Fact]
    public async Task BatchProcessing_ChunksMessagesCorrectly()
    {
        // Arrange
        var messages = Enumerable.Range(0, 25).Select(i => new MessageStoreEntry
        {
            Id = Guid.NewGuid(),
            Direction = MessageDirection.Outbox,
            MessageType = "TestMessage",
            QueueName = $"queue-{i}",
            Payload = System.Text.Encoding.UTF8.GetBytes($"{{}}"),
            Status = MessageStatus.Processing
        }).ToList();

        var publishBatchSize = 10;

        // Act
        var batches = messages.Chunk(publishBatchSize).ToList();

        // Assert
        Assert.Equal(3, batches.Count);
        Assert.Equal(10, batches[0].Length);
        Assert.Equal(10, batches[1].Length);
        Assert.Equal(5, batches[2].Length);
    }

    [Fact]
    public async Task BatchProcessing_SeparatesSuccessesAndFailures()
    {
        // Arrange
        var results = new List<PublishResult>
        {
            PublishResult.Succeeded(Guid.NewGuid()),
            PublishResult.Failed(Guid.NewGuid(), "Error 1"),
            PublishResult.Succeeded(Guid.NewGuid()),
            PublishResult.Failed(Guid.NewGuid(), "Error 2"),
            PublishResult.Succeeded(Guid.NewGuid())
        };

        // Act
        var succeeded = results.Where(r => r.Success).Select(r => r.MessageId).ToList();
        var failed = results.Where(r => !r.Success)
            .Select(r => (r.MessageId, r.Error ?? "Unknown"))
            .ToList();

        // Assert
        Assert.Equal(3, succeeded.Count);
        Assert.Equal(2, failed.Count);
    }

    [Fact]
    public async Task BatchProcessing_FiltersExceededRetryMessages()
    {
        // Arrange
        var maxRetryAttempts = 5;
        var messages = new List<MessageStoreEntry>
        {
            new() { Id = Guid.NewGuid(), RetryCount = 0 },
            new() { Id = Guid.NewGuid(), RetryCount = 3 },
            new() { Id = Guid.NewGuid(), RetryCount = 5 },  // Should be filtered
            new() { Id = Guid.NewGuid(), RetryCount = 4 },
            new() { Id = Guid.NewGuid(), RetryCount = 10 }  // Should be filtered
        };

        // Act
        var messagesToProcess = messages.Where(m => m.RetryCount < maxRetryAttempts).ToList();
        var messagesToFail = messages.Where(m => m.RetryCount >= maxRetryAttempts).ToList();

        // Assert
        Assert.Equal(3, messagesToProcess.Count);
        Assert.Equal(2, messagesToFail.Count);
    }

    #endregion

    #region Repository Batch Methods Tests

    [Fact]
    public async Task OutboxRepository_MarkAsPublishedBatch_CallsProvider()
    {
        // Arrange
        var mockProvider = new Mock<IMessageStoreProvider>();
        var repository = new OutboxRepository(mockProvider.Object);
        var messageIds = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

        // Act
        await repository.MarkAsPublishedBatchAsync(messageIds);

        // Assert
        mockProvider.Verify(p => p.MarkAsPublishedBatchAsync(
            It.Is<IEnumerable<Guid>>(ids => ids.SequenceEqual(messageIds)),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task OutboxRepository_MarkAsFailedBatch_CallsProvider()
    {
        // Arrange
        var mockProvider = new Mock<IMessageStoreProvider>();
        var repository = new OutboxRepository(mockProvider.Object);
        var failures = new[]
        {
            (Guid.NewGuid(), "Error 1"),
            (Guid.NewGuid(), "Error 2")
        };

        // Act
        await repository.MarkAsFailedBatchAsync(failures);

        // Assert
        mockProvider.Verify(p => p.MarkAsFailedBatchAsync(
            It.Is<IEnumerable<(Guid Id, string Error)>>(f => f.Count() == 2),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task OutboxRepository_AcquireLockWithPartitions_CallsProvider()
    {
        // Arrange
        var mockProvider = new Mock<IMessageStoreProvider>();
        mockProvider.Setup(p => p.AcquireOutboxLockAsync(
            It.IsAny<int>(),
            It.IsAny<TimeSpan>(),
            It.IsAny<int[]?>(),
            It.IsAny<int>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<MessageStoreEntry>());

        var repository = new OutboxRepository(mockProvider.Object);
        var partitions = new[] { 0, 2 };

        // Act
        await repository.AcquireLockAsync(100, TimeSpan.FromMinutes(5), partitions, 4);

        // Assert
        mockProvider.Verify(p => p.AcquireOutboxLockAsync(
            100,
            TimeSpan.FromMinutes(5),
            It.Is<int[]?>(p => p != null && p.SequenceEqual(partitions)),
            4,
            It.IsAny<CancellationToken>()), Times.Once);
    }

    #endregion

    #region PublishResult Tests

    [Fact]
    public void PublishResult_Succeeded_CreatesSuccessfulResult()
    {
        // Arrange
        var messageId = Guid.NewGuid();

        // Act
        var result = PublishResult.Succeeded(messageId);

        // Assert
        Assert.True(result.Success);
        Assert.Equal(messageId, result.MessageId);
        Assert.Null(result.Error);
    }

    [Fact]
    public void PublishResult_Failed_CreatesFailedResult()
    {
        // Arrange
        var messageId = Guid.NewGuid();
        var error = "Connection timeout";

        // Act
        var result = PublishResult.Failed(messageId, error);

        // Assert
        Assert.False(result.Success);
        Assert.Equal(messageId, result.MessageId);
        Assert.Equal(error, result.Error);
    }

    #endregion

    #region OutboxOptions Tests

    [Fact]
    public void OutboxOptions_HasCorrectDefaults()
    {
        // Act
        var options = new OutboxOptions();

        // Assert
        Assert.Equal(1, options.WorkerCount);
        Assert.Equal(10, options.PublishBatchSize);
        Assert.Equal(4, options.PartitionCount);

        // Existing defaults should be unchanged
        Assert.True(options.Enabled);
        Assert.Equal(100, options.BatchSize);
        Assert.Equal(TimeSpan.FromSeconds(5), options.ProcessingInterval);
        Assert.Equal(TimeSpan.FromMinutes(5), options.LockDuration);
        Assert.Equal(5, options.MaxRetryAttempts);
    }

    [Fact]
    public void OutboxOptions_CanSetNewProperties()
    {
        // Act
        var options = new OutboxOptions
        {
            WorkerCount = 4,
            PublishBatchSize = 25,
            PartitionCount = 8
        };

        // Assert
        Assert.Equal(4, options.WorkerCount);
        Assert.Equal(25, options.PublishBatchSize);
        Assert.Equal(8, options.PartitionCount);
    }

    #endregion
}
