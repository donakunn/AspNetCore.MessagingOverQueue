using System.Collections.Concurrent;

namespace MessagingOverQueue.Test.Integration.RedisStreams.LoadTests.Metrics;

/// <summary>
/// Thread-safe counter for throughput measurements with time-windowed sampling.
/// Provides both total count and rate-per-second calculations.
/// </summary>
public sealed class ThroughputCounter
{
    private long _totalCount;
    private readonly ConcurrentQueue<(DateTime Timestamp, long Count)> _samples = new();
    private readonly TimeSpan _windowSize;
    private long _windowCount;
    private DateTime _windowStart;
    private readonly object _windowLock = new();

    /// <summary>
    /// Creates a new throughput counter with the specified sampling window.
    /// </summary>
    /// <param name="windowSize">The time window for rate sampling.</param>
    public ThroughputCounter(TimeSpan windowSize)
    {
        _windowSize = windowSize;
        _windowStart = DateTime.UtcNow;
    }

    /// <summary>
    /// Gets the total count of all increments.
    /// </summary>
    public long TotalCount => Volatile.Read(ref _totalCount);

    /// <summary>
    /// Increments the counter by one.
    /// Thread-safe.
    /// </summary>
    public void Increment()
    {
        Interlocked.Increment(ref _totalCount);
        RecordSample(1);
    }

    /// <summary>
    /// Increments the counter by the specified amount.
    /// Thread-safe.
    /// </summary>
    public void IncrementBy(int count)
    {
        Interlocked.Add(ref _totalCount, count);
        RecordSample(count);
    }

    /// <summary>
    /// Gets the average rate per second over a specified period.
    /// </summary>
    /// <param name="period">The period to calculate rate over. If null, uses all samples.</param>
    public double GetAverageRatePerSecond(TimeSpan? period = null)
    {
        // Flush current window
        FlushCurrentWindow();

        var samples = _samples.ToArray();
        if (samples.Length == 0)
        {
            // Calculate from total if no samples
            return 0;
        }

        var cutoff = period.HasValue
            ? DateTime.UtcNow - period.Value
            : samples[0].Timestamp;

        var relevantSamples = samples.Where(s => s.Timestamp >= cutoff).ToArray();
        if (relevantSamples.Length == 0) return 0;

        var totalInPeriod = relevantSamples.Sum(s => s.Count);
        var duration = DateTime.UtcNow - relevantSamples[0].Timestamp;

        return duration.TotalSeconds > 0 ? totalInPeriod / duration.TotalSeconds : 0;
    }

    /// <summary>
    /// Resets the counter to zero.
    /// </summary>
    public void Reset()
    {
        Interlocked.Exchange(ref _totalCount, 0);
        _samples.Clear();
        lock (_windowLock)
        {
            _windowCount = 0;
            _windowStart = DateTime.UtcNow;
        }
    }

    private void RecordSample(int count)
    {
        lock (_windowLock)
        {
            var now = DateTime.UtcNow;
            if (now - _windowStart >= _windowSize)
            {
                _samples.Enqueue((_windowStart, _windowCount));
                _windowStart = now;
                _windowCount = count;

                // Keep only last 120 samples (2 minutes at 1-second windows)
                while (_samples.Count > 120)
                {
                    _samples.TryDequeue(out _);
                }
            }
            else
            {
                _windowCount += count;
            }
        }
    }

    private void FlushCurrentWindow()
    {
        lock (_windowLock)
        {
            if (_windowCount > 0)
            {
                _samples.Enqueue((_windowStart, _windowCount));
                _windowStart = DateTime.UtcNow;
                _windowCount = 0;
            }
        }
    }
}
