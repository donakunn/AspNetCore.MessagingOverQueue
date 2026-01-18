using System.Collections.Concurrent;
using System.Diagnostics;
using Donakunn.MessagingOverQueue.Abstractions.Consuming;
using Donakunn.MessagingOverQueue.Topology.Attributes;
using MessagingOverQueue.Test.Integration.Shared.Infrastructure;
using MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Metrics;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.TestMessages;

/// <summary>
/// Handler for FailingLoadTestEvent with failure injection and metrics tracking.
/// Tracks attempt counts per message ID and can simulate transient failures that recover after retries.
/// Uses TestExecutionContext for test isolation in parallel execution.
/// </summary>
[ConsumerQueue(MaxConcurrency = 50)]
public class FailingLoadTestEventHandler : IMessageHandler<FailingLoadTestEvent>
{
    private const string HandlerKey = nameof(FailingLoadTestEventHandler);
    private const string AttemptsKey = HandlerKey + "_Attempts";
    private const string MetricsCollectorKey = HandlerKey + "_MetricsCollector";
    private const string ErrorCountKey = HandlerKey + "_ErrorCount";

    /// <summary>
    /// Gets the total number of messages successfully handled.
    /// </summary>
    public static long HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;

    /// <summary>
    /// Gets the total number of errors thrown.
    /// </summary>
    public static long ErrorCount => TestExecutionContextAccessor.GetRequired().GetCounter(ErrorCountKey).Count;

    /// <summary>
    /// Gets the attempt counts per message ID.
    /// </summary>
    public static IReadOnlyDictionary<Guid, int> AttemptCounts
    {
        get
        {
            var dict = TestExecutionContextAccessor.GetRequired().GetCustomData<ConcurrentDictionary<Guid, int>>(AttemptsKey);
            return dict ?? new ConcurrentDictionary<Guid, int>();
        }
    }

    /// <summary>
    /// Sets the metrics collector for recording metrics during tests.
    /// </summary>
    public static void SetMetricsCollector(IMetricsCollector? collector)
    {
        TestExecutionContextAccessor.GetRequired().SetCustomData(MetricsCollectorKey, collector!);
    }

    /// <summary>
    /// Resets counters and attempt tracking for test isolation.
    /// </summary>
    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
        context.GetCounter(ErrorCountKey).Reset();
        context.SetCustomData(AttemptsKey, new ConcurrentDictionary<Guid, int>());
    }

    /// <summary>
    /// Waits for the handler to reach the expected successful handle count.
    /// </summary>
    public static Task WaitForCountAsync(long expected, TimeSpan timeout)
        => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).WaitForCountAsync((int)expected, timeout);

    public async Task HandleAsync(FailingLoadTestEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        var testContext = TestExecutionContextAccessor.GetRequired();
        var metricsCollector = testContext.GetCustomData<IMetricsCollector>(MetricsCollectorKey);

        // Get or create attempt tracking dictionary
        var attempts = testContext.GetCustomData<ConcurrentDictionary<Guid, int>>(AttemptsKey);
        if (attempts == null)
        {
            attempts = new ConcurrentDictionary<Guid, int>();
            testContext.SetCustomData(AttemptsKey, attempts);
        }

        // Increment attempt count for this message
        var attemptNumber = attempts.AddOrUpdate(message.Id, 1, (_, count) => count + 1);

        // Calculate latency
        var receivedTicks = Stopwatch.GetTimestamp();
        var latencyTicks = receivedTicks - message.PublishedAtTicks;
        var latency = TimeSpan.FromTicks(latencyTicks * TimeSpan.TicksPerSecond / Stopwatch.Frequency);

        // Simulate processing delay (for timeout testing)
        if (message.SimulatedDelay > TimeSpan.Zero)
        {
            await Task.Delay(message.SimulatedDelay, cancellationToken);
        }

        // Determine if we should fail
        bool shouldFail = message.ShouldFail;

        if (shouldFail && message.FailOnAttempts.Count > 0)
        {
            // Only fail on specified attempts
            shouldFail = message.FailOnAttempts.Contains(attemptNumber);
        }
        else if (shouldFail && message.IsTransient && attemptNumber > 3)
        {
            // For transient failures without specific attempts, succeed after 3 retries
            shouldFail = false;
        }

        if (shouldFail)
        {
            testContext.GetCounter(ErrorCountKey).Increment();
            metricsCollector?.RecordError(message.IsTransient ? "TransientFailure" : "PermanentFailure");

            throw new InvalidOperationException(
                $"{message.FailureMessage} (MessageId: {message.Id}, Attempt: {attemptNumber}, Sequence: {message.Sequence})");
        }

        // Success path
        testContext.GetCounter(HandlerKey).Increment();

        metricsCollector?.RecordLatency(latency);
        metricsCollector?.RecordConsumed();
    }
}
