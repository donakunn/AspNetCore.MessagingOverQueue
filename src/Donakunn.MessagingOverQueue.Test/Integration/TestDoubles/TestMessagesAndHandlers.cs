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
    private static readonly HandlerCounter Counter = new();
    private static readonly MessageCollector<SimpleTestEvent> Collector = new();

    public static int HandleCount => Counter.Count;
    public static IReadOnlyCollection<SimpleTestEvent> HandledMessages => Collector.Messages;

    public static void Reset()
    {
        Counter.Reset();
        Collector.Clear();
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => Counter.WaitForCountAsync(expected, timeout);

    public Task HandleAsync(SimpleTestEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        Counter.Increment();
        Collector.Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler for complex event tests.
/// </summary>
public class ComplexTestEventHandler : IMessageHandler<ComplexTestEvent>
{
    private static readonly HandlerCounter Counter = new();
    private static readonly MessageCollector<ComplexTestEvent> Collector = new();

    public static int HandleCount => Counter.Count;
    public static IReadOnlyCollection<ComplexTestEvent> HandledMessages => Collector.Messages;

    public static void Reset()
    {
        Counter.Reset();
        Collector.Clear();
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => Counter.WaitForCountAsync(expected, timeout);

    public Task HandleAsync(ComplexTestEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        Counter.Increment();
        message.ProcessedAt = DateTime.UtcNow;
        Collector.Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler that simulates slow processing.
/// </summary>
public class SlowProcessingEventHandler : IMessageHandler<SlowProcessingEvent>
{
    private static readonly HandlerCounter Counter = new();
    private static readonly MessageCollector<SlowProcessingEvent> Collector = new();

    public static int HandleCount => Counter.Count;
    public static IReadOnlyCollection<SlowProcessingEvent> HandledMessages => Collector.Messages;

    public static void Reset()
    {
        Counter.Reset();
        Collector.Clear();
    }

    public async Task HandleAsync(SlowProcessingEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        await Task.Delay(message.ProcessingTime, cancellationToken);
        Counter.Increment();
        Collector.Add(message);
    }
}

/// <summary>
/// Handler that throws exceptions for error handling tests.
/// </summary>
public class FailingEventHandler : IMessageHandler<FailingEvent>
{
    private static readonly HandlerCounter Counter = new();
    private static readonly HandlerCounter FailureCounter = new();
    private static readonly ExceptionCollector Exceptions = new();

    public static int HandleCount => Counter.Count;
    public static int FailureCount => FailureCounter.Count;
    public static IReadOnlyCollection<Exception> ThrownExceptions => Exceptions.Exceptions;

    public static void Reset()
    {
        Counter.Reset();
        FailureCounter.Reset();
        Exceptions.Clear();
    }

    public Task HandleAsync(FailingEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        Counter.Increment();

        if (message.ShouldFail)
        {
            FailureCounter.Increment();
            var ex = new InvalidOperationException(message.FailureMessage);
            Exceptions.Add(ex);
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
    private static readonly HandlerCounter Counter = new();
    private static readonly MessageCollector<ConcurrencyTestEvent> Collector = new();
    private static readonly object Lock = new();
    private static int _maxConcurrent;
    private static int _currentConcurrent;

    public static int HandleCount => Counter.Count;
    public static int MaxConcurrentObserved => Volatile.Read(ref _maxConcurrent);
    public static IReadOnlyCollection<ConcurrencyTestEvent> HandledMessages => Collector.Messages;

    public static void Reset()
    {
        Counter.Reset();
        Collector.Clear();
        _maxConcurrent = 0;
        _currentConcurrent = 0;
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => Counter.WaitForCountAsync(expected, timeout);

    public async Task HandleAsync(ConcurrencyTestEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        var concurrent = Interlocked.Increment(ref _currentConcurrent);

        lock (Lock)
        {
            if (concurrent > _maxConcurrent)
            {
                _maxConcurrent = concurrent;
            }
        }

        try
        {
            message.ThreadInfo = $"Thread-{Environment.CurrentManagedThreadId}";
            await Task.Delay(50, cancellationToken); // Simulate work
            Counter.Increment();
            Collector.Add(message);
        }
        finally
        {
            Interlocked.Decrement(ref _currentConcurrent);
        }
    }
}

/// <summary>
/// First handler for multi-handler event.
/// </summary>
public class MultiHandlerEventHandlerA : IMessageHandler<MultiHandlerEvent>
{
    private static readonly HandlerCounter Counter = new();
    private static readonly MessageCollector<MultiHandlerEvent> Collector = new();

    public static int HandleCount => Counter.Count;
    public static IReadOnlyCollection<MultiHandlerEvent> HandledMessages => Collector.Messages;

    public static void Reset()
    {
        Counter.Reset();
        Collector.Clear();
    }

    public Task HandleAsync(MultiHandlerEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        Counter.Increment();
        Collector.Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Second handler for multi-handler event.
/// </summary>
public class MultiHandlerEventHandlerB : IMessageHandler<MultiHandlerEvent>
{
    private static readonly HandlerCounter Counter = new();
    private static readonly MessageCollector<MultiHandlerEvent> Collector = new();

    public static int HandleCount => Counter.Count;
    public static IReadOnlyCollection<MultiHandlerEvent> HandledMessages => Collector.Messages;

    public static void Reset()
    {
        Counter.Reset();
        Collector.Clear();
    }

    public Task HandleAsync(MultiHandlerEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        Counter.Increment();
        Collector.Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler for ordered events.
/// </summary>
public class OrderedEventHandler : IMessageHandler<OrderedEvent>
{
    private static readonly HandlerCounter Counter = new();
    private static readonly MessageCollector<(int Sequence, DateTime ProcessedAt)> OrderCollector = new();

    public static int HandleCount => Counter.Count;
    public static IReadOnlyCollection<(int Sequence, DateTime ProcessedAt)> ProcessOrder => OrderCollector.Messages;

    public static void Reset()
    {
        Counter.Reset();
        OrderCollector.Clear();
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => Counter.WaitForCountAsync(expected, timeout);

    public Task HandleAsync(OrderedEvent message, IMessageContext context, CancellationToken cancellationToken)
    {
        Counter.Increment();
        OrderCollector.Add((message.Sequence, DateTime.UtcNow));
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler for simple commands.
/// </summary>
public class SimpleTestCommandHandler : IMessageHandler<SimpleTestCommand>
{
    private static readonly HandlerCounter Counter = new();
    private static readonly MessageCollector<SimpleTestCommand> Collector = new();

    public static int HandleCount => Counter.Count;
    public static IReadOnlyCollection<SimpleTestCommand> HandledCommands => Collector.Messages;

    public static void Reset()
    {
        Counter.Reset();
        Collector.Clear();
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => Counter.WaitForCountAsync(expected, timeout);

    public Task HandleAsync(SimpleTestCommand message, IMessageContext context, CancellationToken cancellationToken)
    {
        Counter.Increment();
        Collector.Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Handler for ProcessOrderCommand.
/// </summary>
public class ProcessOrderCommandHandler : IMessageHandler<ProcessOrderCommand>
{
    private static readonly HandlerCounter Counter = new();
    private static readonly MessageCollector<ProcessOrderCommand> Collector = new();

    public static int HandleCount => Counter.Count;
    public static IReadOnlyCollection<ProcessOrderCommand> ProcessedOrders => Collector.Messages;

    public static void Reset()
    {
        Counter.Reset();
        Collector.Clear();
    }

    public static Task WaitForCountAsync(int expected, TimeSpan timeout)
        => Counter.WaitForCountAsync(expected, timeout);

    public Task HandleAsync(ProcessOrderCommand message, IMessageContext context, CancellationToken cancellationToken)
    {
        Counter.Increment();
        Collector.Add(message);
        return Task.CompletedTask;
    }
}

#endregion
