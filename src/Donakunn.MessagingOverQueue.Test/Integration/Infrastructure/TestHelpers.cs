using System.Collections.Concurrent;

namespace MessagingOverQueue.Test.Integration.Infrastructure;

/// <summary>
/// Thread-safe counter for tracking handler invocations in tests.
/// </summary>
public sealed class HandlerCounter
{
    private int _count;
    
    public int Count => Volatile.Read(ref _count);
    
    public void Increment() => Interlocked.Increment(ref _count);
    
    public void Reset() => Interlocked.Exchange(ref _count, 0);
    
    public async Task WaitForCountAsync(int expectedCount, TimeSpan timeout)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (Count < expectedCount && sw.Elapsed < timeout)
        {
            await Task.Delay(50);
        }
    }
}

/// <summary>
/// Thread-safe message collector for tracking handled messages in tests.
/// </summary>
/// <typeparam name="T">The message type to collect.</typeparam>
public sealed class MessageCollector<T>
{
    private readonly ConcurrentBag<T> _messages = [];
    private readonly ConcurrentBag<DateTime> _timestamps = [];
    
    public IReadOnlyCollection<T> Messages => _messages.ToArray();
    public int Count => _messages.Count;
    
    public void Add(T message)
    {
        _messages.Add(message);
        _timestamps.Add(DateTime.UtcNow);
    }
    
    public void Clear()
    {
        _messages.Clear();
        _timestamps.Clear();
    }
    
    public async Task WaitForCountAsync(int expectedCount, TimeSpan timeout)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (Count < expectedCount && sw.Elapsed < timeout)
        {
            await Task.Delay(50);
        }
    }
    
    public bool Contains(Func<T, bool> predicate) => _messages.Any(predicate);
}

/// <summary>
/// Thread-safe exception collector for tracking errors in tests.
/// </summary>
public sealed class ExceptionCollector
{
    private readonly ConcurrentBag<Exception> _exceptions = [];
    
    public IReadOnlyCollection<Exception> Exceptions => _exceptions.ToArray();
    public int Count => _exceptions.Count;
    public bool HasExceptions => !_exceptions.IsEmpty;
    
    public void Add(Exception exception) => _exceptions.Add(exception);
    
    public void Clear() => _exceptions.Clear();
}

/// <summary>
/// Thread-safe latch for coordinating concurrent test operations.
/// </summary>
public sealed class TestLatch
{
    private readonly SemaphoreSlim _semaphore;
    private readonly int _count;
    private int _arrived;
    
    public TestLatch(int count)
    {
        _count = count;
        _semaphore = new SemaphoreSlim(0, count);
    }
    
    public void Signal()
    {
        var arrived = Interlocked.Increment(ref _arrived);
        if (arrived <= _count)
        {
            _semaphore.Release();
        }
    }
    
    public async Task WaitAllAsync(TimeSpan timeout)
    {
        for (int i = 0; i < _count; i++)
        {
            if (!await _semaphore.WaitAsync(timeout))
            {
                throw new TimeoutException($"Latch timeout - only {i} of {_count} signals received.");
            }
        }
    }
}

/// <summary>
/// Test barrier for synchronizing multiple concurrent operations.
/// </summary>
public sealed class TestBarrier : IDisposable
{
    private readonly Barrier _barrier;
    
    public TestBarrier(int participantCount)
    {
        _barrier = new Barrier(participantCount);
    }
    
    public void SignalAndWait(TimeSpan timeout)
    {
        if (!_barrier.SignalAndWait(timeout))
        {
            throw new TimeoutException("Barrier timeout waiting for all participants.");
        }
    }
    
    public async Task SignalAndWaitAsync(TimeSpan timeout)
    {
        await Task.Run(() => SignalAndWait(timeout));
    }
    
    public void Dispose() => _barrier.Dispose();
}

/// <summary>
/// Async manual reset event for test coordination.
/// </summary>
public sealed class AsyncManualResetEvent
{
    private volatile TaskCompletionSource<bool> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task WaitAsync() => _tcs.Task;
    
    public Task WaitAsync(TimeSpan timeout)
    {
        return Task.WhenAny(_tcs.Task, Task.Delay(timeout));
    }

    public void Set() => _tcs.TrySetResult(true);

    public void Reset()
    {
        while (true)
        {
            var tcs = _tcs;
            if (!tcs.Task.IsCompleted || 
                Interlocked.CompareExchange(ref _tcs, new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously), tcs) == tcs)
            {
                return;
            }
        }
    }
}
