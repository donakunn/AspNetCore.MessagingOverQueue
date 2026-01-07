using Polly;
using Polly.CircuitBreaker;

namespace MessagingOverQueue.src.Resilience.CircuitBreaker;

/// <summary>
/// Polly-based circuit breaker implementation using Polly v8 with enhanced thread safety and observability.
/// </summary>
public sealed class PollyCircuitBreaker : ICircuitBreaker, IDisposable
{
    private readonly ResiliencePipeline _pipeline;
    private readonly Lock _stateLock = new();
    private CircuitState _currentState = CircuitState.Closed;
    private readonly SemaphoreSlim _disposeSemaphore = new(1, 1);
    private bool _disposed;

    public event EventHandler<CircuitStateChangedEventArgs>? StateChanged;

    public PollyCircuitBreaker(CircuitBreakerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        _pipeline = new ResiliencePipelineBuilder()
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                FailureRatio = options.FailureRateThreshold,
                SamplingDuration = options.SamplingDuration,
                MinimumThroughput = options.MinimumThroughput,
                BreakDuration = options.DurationOfBreak,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(),
                OnOpened = args =>
                {
                    UpdateState(CircuitState.Open, args.Outcome.Exception);
                    return ValueTask.CompletedTask;
                },
                OnClosed = args =>
                {
                    UpdateState(CircuitState.Closed);
                    return ValueTask.CompletedTask;
                },
                OnHalfOpened = args =>
                {
                    UpdateState(CircuitState.HalfOpen);
                    return ValueTask.CompletedTask;
                }
            })
            .Build();
    }

    public CircuitState State
    {
        get
        {
            lock (_stateLock)
            {
                return _currentState;
            }
        }
    }

    public async Task ExecuteAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(action);
        ThrowIfDisposed();

        try
        {
            await _pipeline.ExecuteAsync(async ct => await action(ct), cancellationToken).ConfigureAwait(false);
        }
        catch (BrokenCircuitException ex)
        {
            throw new CircuitBreakerOpenException("Circuit breaker is open and not accepting requests", ex);
        }
    }

    public async Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(action);
        ThrowIfDisposed();

        try
        {
            return await _pipeline.ExecuteAsync(async ct => await action(ct), cancellationToken).ConfigureAwait(false);
        }
        catch (BrokenCircuitException ex)
        {
            throw new CircuitBreakerOpenException("Circuit breaker is open and not accepting requests", ex);
        }
    }

    private void UpdateState(CircuitState newState, Exception? exception = null)
    {
        CircuitState previousState;
        CircuitStateChangedEventArgs? eventArgs;

        lock (_stateLock)
        {
            if (_currentState == newState)
                return;

            previousState = _currentState;
            _currentState = newState;
            eventArgs = new CircuitStateChangedEventArgs(previousState, newState, exception);
        }

        // Raise event outside the lock to prevent deadlocks
        StateChanged?.Invoke(this, eventArgs);
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(PollyCircuitBreaker));
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposeSemaphore.Wait();
        try
        {
            if (_disposed)
                return;

            _disposed = true;
            _disposeSemaphore.Dispose();
        }
        finally
        {
            if (!_disposed)
                _disposeSemaphore.Release();
        }
    }
}
