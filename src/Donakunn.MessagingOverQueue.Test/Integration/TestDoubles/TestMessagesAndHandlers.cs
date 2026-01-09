using Donakunn.MessagingOverQueue.Abstractions.Consuming;
using Donakunn.MessagingOverQueue.Abstractions.Messages;
using Donakunn.MessagingOverQueue.Topology.Attributes;
using MessagingOverQueue.Test.Integration.Infrastructure;

namespace MessagingOverQueue.Test.Integration.TestDoubles;

#region Test Events

/// <summary>
/// Simple test event for basic handler tests.
/// </summary>
public class SimpleTestEvent : Event
{
    public string Value { get; set; } = string.Empty;
}

/// <summary>
/// Test event with complex payload.
/// </summary>
public class ComplexTestEvent : Event
{
    public string Name { get; set; } = string.Empty;
    public int Count { get; set; }
    public decimal Amount { get; set; }
    public DateTime? ProcessedAt { get; set; }
    public List<string> Tags { get; set; } = [];
    public Dictionary<string, object> Metadata { get; set; } = new();
}

/// <summary>
/// Event for testing slow processing.
/// </summary>
public class SlowProcessingEvent : Event
{
    public TimeSpan ProcessingTime { get; set; } = TimeSpan.FromMilliseconds(100);
    public string Value { get; set; } = string.Empty;
}

/// <summary>
/// Event for testing error handling.
/// </summary>
public class FailingEvent : Event
{
    public bool ShouldFail { get; set; } = true;
    public string FailureMessage { get; set; } = "Intentional test failure";
}

/// <summary>
/// Event for concurrency testing.
/// </summary>
public class ConcurrencyTestEvent : Event
{
    public int Index { get; set; }
    public string ThreadInfo { get; set; } = string.Empty;
}

/// <summary>
/// Event with custom queue configuration.
/// </summary>
[Queue("custom-queue", QueueType = QueueType.Classic)]
[RoutingKey("test.custom")]
public class CustomQueueEvent : Event
{
    public string Data { get; set; } = string.Empty;
}

/// <summary>
/// High priority event.
/// </summary>
public class HighPriorityEvent : Event
{
    public int Priority { get; set; } = 10;
    public string Message { get; set; } = string.Empty;
}

/// <summary>
/// Event for testing multiple handlers.
/// </summary>
public class MultiHandlerEvent : Event
{
    public string Payload { get; set; } = string.Empty;
}

/// <summary>
/// Event for ordering tests.
/// </summary>
public class OrderedEvent : Event
{
    public int Sequence { get; set; }
    public DateTime EnqueuedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// Event for testing idempotency.
/// </summary>
public class IdempotentTestEvent : Event
{
    public string Value { get; set; } = string.Empty;
}

#endregion

#region Test Commands

/// <summary>
/// Simple test command.
/// </summary>
public class SimpleTestCommand : Command
{
    public string Action { get; set; } = string.Empty;
}

/// <summary>
/// Command for testing command handling.
/// </summary>
public class ProcessOrderCommand : Command
{
    public Guid OrderId { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public decimal Total { get; set; }
}

#endregion

#region Test Handlers

/// <summary>
/// Simple handler for basic event tests.
/// </summary>
public class SimpleTestEventHandler : IMessageHandler<SimpleTestEvent>
{
    private const string HandlerKey = nameof(SimpleTestEventHandler);

    public static int HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;
    public static IReadOnlyCollection<SimpleTestEvent> HandledMessages => 
        TestExecutionContextAccessor.GetRequired().GetCollector<SimpleTestEvent>(HandlerKey).Messages;

    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
        context.GetCollector<SimpleTestEvent>(HandlerKey).Clear();
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).WaitForCountAsync(expected, timeout);

    public Task HandleAsync(SimpleTestEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        var testContext = TestExecutionContextAccessor.GetRequired();
        testContext.GetCounter(HandlerKey).Increment();
        testContext.GetCollector<SimpleTestEvent>(HandlerKey).Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler for complex event tests.
/// </summary>
public class ComplexTestEventHandler : IMessageHandler<ComplexTestEvent>
{
    private const string HandlerKey = nameof(ComplexTestEventHandler);

    public static int HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;
    public static IReadOnlyCollection<ComplexTestEvent> HandledMessages => 
        TestExecutionContextAccessor.GetRequired().GetCollector<ComplexTestEvent>(HandlerKey).Messages;

    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
        context.GetCollector<ComplexTestEvent>(HandlerKey).Clear();
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).WaitForCountAsync(expected, timeout);

    public Task HandleAsync(ComplexTestEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        var testContext = TestExecutionContextAccessor.GetRequired();
        testContext.GetCounter(HandlerKey).Increment();
        message.ProcessedAt = DateTime.UtcNow;
        testContext.GetCollector<ComplexTestEvent>(HandlerKey).Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler that simulates slow processing.
/// </summary>
public class SlowProcessingEventHandler : IMessageHandler<SlowProcessingEvent>
{
    private const string HandlerKey = nameof(SlowProcessingEventHandler);

    public static int HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;
    public static IReadOnlyCollection<SlowProcessingEvent> HandledMessages => 
        TestExecutionContextAccessor.GetRequired().GetCollector<SlowProcessingEvent>(HandlerKey).Messages;

    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
        context.GetCollector<SlowProcessingEvent>(HandlerKey).Clear();
    }

    public async Task HandleAsync(SlowProcessingEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        await Task.Delay(message.ProcessingTime, cancellationToken);
        var testContext = TestExecutionContextAccessor.GetRequired();
        testContext.GetCounter(HandlerKey).Increment();
        testContext.GetCollector<SlowProcessingEvent>(HandlerKey).Add(message);
    }
}

/// <summary>
/// Handler that throws exceptions for error handling tests.
/// </summary>
public class FailingEventHandler : IMessageHandler<FailingEvent>
{
    private const string HandlerKey = nameof(FailingEventHandler);
    private const string FailureCountKey = nameof(FailingEventHandler) + "_FailureCount";

    public static int HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;
    public static int FailureCount => TestExecutionContextAccessor.GetRequired().GetCounter(FailureCountKey).Count;
    public static IReadOnlyCollection<Exception> ThrownExceptions => 
        TestExecutionContextAccessor.GetRequired().GetExceptionCollector(HandlerKey).Exceptions;

    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
        context.GetCounter(FailureCountKey).Reset();
        context.GetExceptionCollector(HandlerKey).Clear();
    }

    public Task HandleAsync(FailingEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        var testContext = TestExecutionContextAccessor.GetRequired();
        testContext.GetCounter(HandlerKey).Increment();

        if (message.ShouldFail)
        {
            testContext.GetCounter(FailureCountKey).Increment();
            var ex = new InvalidOperationException(message.FailureMessage);
            testContext.GetExceptionCollector(HandlerKey).Add(ex);
            throw ex;
        }

        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler for concurrency testing.
/// </summary>
public class ConcurrencyTestEventHandler : IMessageHandler<ConcurrencyTestEvent>
{
    private const string HandlerKey = nameof(ConcurrencyTestEventHandler);
    private const string MaxConcurrentKey = HandlerKey + "_MaxConcurrent";
    private const string CurrentConcurrentKey = HandlerKey + "_CurrentConcurrent";

    public static int HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;
    public static int MaxConcurrentObserved => 
        TestExecutionContextAccessor.GetRequired().GetCustomData<int>(MaxConcurrentKey);
    public static IReadOnlyCollection<ConcurrencyTestEvent> HandledMessages => 
        TestExecutionContextAccessor.GetRequired().GetCollector<ConcurrencyTestEvent>(HandlerKey).Messages;

    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
        context.GetCollector<ConcurrencyTestEvent>(HandlerKey).Clear();
        context.SetCustomData(MaxConcurrentKey, 0);
        context.SetCustomData(CurrentConcurrentKey, 0);
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).WaitForCountAsync(expected, timeout);

    public async Task HandleAsync(ConcurrencyTestEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        var testContext = TestExecutionContextAccessor.GetRequired();
        var currentConcurrent = testContext.GetCustomData<int>(CurrentConcurrentKey) + 1;
        testContext.SetCustomData(CurrentConcurrentKey, currentConcurrent);

        var maxConcurrent = testContext.GetCustomData<int>(MaxConcurrentKey);
        if (currentConcurrent > maxConcurrent)
        {
            testContext.SetCustomData(MaxConcurrentKey, currentConcurrent);
        }

        try
        {
            message.ThreadInfo = $"Thread-{Environment.CurrentManagedThreadId}";
            await Task.Delay(50, cancellationToken); // Simulate work
            testContext.GetCounter(HandlerKey).Increment();
            testContext.GetCollector<ConcurrencyTestEvent>(HandlerKey).Add(message);
        }
        finally
        {
            var decremented = testContext.GetCustomData<int>(CurrentConcurrentKey) - 1;
            testContext.SetCustomData(CurrentConcurrentKey, decremented);
        }
    }
}

/// <summary>
/// First handler for multi-handler event.
/// </summary>
public class MultiHandlerEventHandlerA : IMessageHandler<MultiHandlerEvent>
{
    private const string HandlerKey = nameof(MultiHandlerEventHandlerA);

    public static int HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;
    public static IReadOnlyCollection<MultiHandlerEvent> HandledMessages => 
        TestExecutionContextAccessor.GetRequired().GetCollector<MultiHandlerEvent>(HandlerKey).Messages;

    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
        context.GetCollector<MultiHandlerEvent>(HandlerKey).Clear();
    }

    public Task HandleAsync(MultiHandlerEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        var testContext = TestExecutionContextAccessor.GetRequired();
        testContext.GetCounter(HandlerKey).Increment();
        testContext.GetCollector<MultiHandlerEvent>(HandlerKey).Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Second handler for multi-handler event.
/// </summary>
public class MultiHandlerEventHandlerB : IMessageHandler<MultiHandlerEvent>
{
    private const string HandlerKey = nameof(MultiHandlerEventHandlerB);

    public static int HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;
    public static IReadOnlyCollection<MultiHandlerEvent> HandledMessages => 
        TestExecutionContextAccessor.GetRequired().GetCollector<MultiHandlerEvent>(HandlerKey).Messages;

    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
        context.GetCollector<MultiHandlerEvent>(HandlerKey).Clear();
    }

    public Task HandleAsync(MultiHandlerEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        var testContext = TestExecutionContextAccessor.GetRequired();
        testContext.GetCounter(HandlerKey).Increment();
        testContext.GetCollector<MultiHandlerEvent>(HandlerKey).Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler for ordered events.
/// </summary>
public class OrderedEventHandler : IMessageHandler<OrderedEvent>
{
    private const string HandlerKey = nameof(OrderedEventHandler);

    public static int HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;
    public static IReadOnlyCollection<(int Sequence, DateTime ProcessedAt)> ProcessOrder => 
        TestExecutionContextAccessor.GetRequired().GetCollector<(int Sequence, DateTime ProcessedAt)>(HandlerKey).Messages;

    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
        context.GetCollector<(int Sequence, DateTime ProcessedAt)>(HandlerKey).Clear();
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).WaitForCountAsync(expected, timeout);

    public Task HandleAsync(OrderedEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        var testContext = TestExecutionContextAccessor.GetRequired();
        testContext.GetCounter(HandlerKey).Increment();
        testContext.GetCollector<(int Sequence, DateTime ProcessedAt)>(HandlerKey).Add((message.Sequence, DateTime.UtcNow));
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler for simple commands.
/// </summary>
public class SimpleTestCommandHandler : IMessageHandler<SimpleTestCommand>
{
    private const string HandlerKey = nameof(SimpleTestCommandHandler);

    public static int HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;
    public static IReadOnlyCollection<SimpleTestCommand> HandledCommands => 
        TestExecutionContextAccessor.GetRequired().GetCollector<SimpleTestCommand>(HandlerKey).Messages;

    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
        context.GetCollector<SimpleTestCommand>(HandlerKey).Clear();
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).WaitForCountAsync(expected, timeout);

    public Task HandleAsync(SimpleTestCommand message, IMessageContext context, CancellationToken cancellationToken)
    {
        var testContext = TestExecutionContextAccessor.GetRequired();
        testContext.GetCounter(HandlerKey).Increment();
        testContext.GetCollector<SimpleTestCommand>(HandlerKey).Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler for ProcessOrderCommand.
/// </summary>
public class ProcessOrderCommandHandler : IMessageHandler<ProcessOrderCommand>
{
    private const string HandlerKey = nameof(ProcessOrderCommandHandler);

    public static int HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;
    public static IReadOnlyCollection<ProcessOrderCommand> ProcessedOrders => 
        TestExecutionContextAccessor.GetRequired().GetCollector<ProcessOrderCommand>(HandlerKey).Messages;

    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
        context.GetCollector<ProcessOrderCommand>(HandlerKey).Clear();
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).WaitForCountAsync(expected, timeout);

    public Task HandleAsync(ProcessOrderCommand message, IMessageContext context, CancellationToken cancellationToken)
    {
        var testContext = TestExecutionContextAccessor.GetRequired();
        testContext.GetCounter(HandlerKey).Increment();
        testContext.GetCollector<ProcessOrderCommand>(HandlerKey).Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler for idempotency test events.
/// </summary>
public class IdempotentTestEventHandler : IMessageHandler<IdempotentTestEvent>
{
    private const string HandlerKey = nameof(IdempotentTestEventHandler);

    public static int HandleCount => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).Count;
    public static IReadOnlyCollection<IdempotentTestEvent> HandledMessages => 
        TestExecutionContextAccessor.GetRequired().GetCollector<IdempotentTestEvent>(HandlerKey).Messages;

    public static void Reset()
    {
        var context = TestExecutionContextAccessor.GetRequired();
        context.GetCounter(HandlerKey).Reset();
        context.GetCollector<IdempotentTestEvent>(HandlerKey).Clear();
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => TestExecutionContextAccessor.GetRequired().GetCounter(HandlerKey).WaitForCountAsync(expected, timeout);

    public Task HandleAsync(IdempotentTestEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        var testContext = TestExecutionContextAccessor.GetRequired();
        testContext.GetCounter(HandlerKey).Increment();
        testContext.GetCollector<IdempotentTestEvent>(HandlerKey).Add(message);
        return Task.CompletedTask;
    }
}

#endregion
