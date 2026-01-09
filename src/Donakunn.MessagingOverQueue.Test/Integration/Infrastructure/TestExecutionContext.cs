using System.Collections.Concurrent;

namespace MessagingOverQueue.Test.Integration.Infrastructure;

/// <summary>
/// Provides isolated execution context for test handlers to avoid shared state issues in parallel test execution.
/// Each test gets its own context instance, ensuring complete isolation.
/// </summary>
public class TestExecutionContext
{
    private readonly ConcurrentDictionary<string, HandlerCounter> _counters = new();
    private readonly ConcurrentDictionary<string, object> _collectors = new();
    private readonly ConcurrentDictionary<string, ExceptionCollector> _exceptionCollectors = new();
    private readonly ConcurrentDictionary<string, object> _customData = new();

    /// <summary>
    /// Gets or creates a counter for a specific handler.
    /// </summary>
    public HandlerCounter GetCounter(string handlerName)
    {
        return _counters.GetOrAdd(handlerName, _ => new HandlerCounter());
    }

    /// <summary>
    /// Gets or creates a message collector for a specific handler.
    /// </summary>
    public MessageCollector<T> GetCollector<T>(string handlerName)
    {
        return (MessageCollector<T>)_collectors.GetOrAdd(handlerName, _ => new MessageCollector<T>());
    }

    /// <summary>
    /// Gets or creates an exception collector for a specific handler.
    /// </summary>
    public ExceptionCollector GetExceptionCollector(string handlerName)
    {
        return _exceptionCollectors.GetOrAdd(handlerName, _ => new ExceptionCollector());
    }

    /// <summary>
    /// Stores custom data for a handler (e.g., max concurrent count, custom state).
    /// </summary>
    public void SetCustomData<T>(string key, T value) where T : notnull
    {
        _customData[key] = value;
    }

    /// <summary>
    /// Retrieves custom data for a handler.
    /// </summary>
    public T? GetCustomData<T>(string key)
    {
        return _customData.TryGetValue(key, out var value) ? (T)value : default;
    }

    /// <summary>
    /// Clears all data in this context.
    /// </summary>
    public void Reset()
    {
        foreach (var counter in _counters.Values)
        {
            counter.Reset();
        }

        _counters.Clear();
        _collectors.Clear();
        _exceptionCollectors.Clear();
        _customData.Clear();

        foreach (var collector in _exceptionCollectors.Values)
        {
            collector.Clear();
        }
    }
}
