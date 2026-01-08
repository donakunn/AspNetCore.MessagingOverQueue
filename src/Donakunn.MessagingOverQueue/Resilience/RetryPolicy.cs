using Donakunn.MessagingOverQueue.Configuration.Options;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace Donakunn.MessagingOverQueue.Resilience;

/// <summary>
/// Interface for retry policies.
/// </summary>
public interface IRetryPolicy
{
    /// <summary>
    /// Executes an action with retry logic.
    /// </summary>
    Task ExecuteAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a function with retry logic.
    /// </summary>
    Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken = default);
}

/// <summary>
/// Polly-based retry policy implementation using Polly v8.
/// </summary>
public class PollyRetryPolicy : IRetryPolicy
{
    private readonly ResiliencePipeline _pipeline;

    public PollyRetryPolicy(IOptions<RetryOptions> options)
    {
        var retryOptions = options.Value;

        _pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = retryOptions.MaxRetryAttempts,
                BackoffType = retryOptions.UseExponentialBackoff
                    ? DelayBackoffType.Exponential
                    : DelayBackoffType.Constant,
                Delay = retryOptions.InitialDelay,
                MaxDelay = retryOptions.MaxDelay,
                UseJitter = retryOptions.AddJitter,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(ex => IsRetryable(ex, retryOptions))
            })
            .Build();
    }

    public async Task ExecuteAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken = default)
    {
        await _pipeline.ExecuteAsync(async ct => await action(ct), cancellationToken);
    }

    public async Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken = default)
    {
        return await _pipeline.ExecuteAsync(async ct => await action(ct), cancellationToken);
    }

    private static bool IsRetryable(Exception exception, RetryOptions options)
    {
        // Don't retry if the exception type is in the non-retryable list
        var exceptionTypeName = exception.GetType().FullName ?? exception.GetType().Name;
        if (options.NonRetryableExceptions.Contains(exceptionTypeName))
            return false;

        // Retry transient errors
        return exception is TimeoutException or
            OperationCanceledException or
            IOException or
            System.Net.Sockets.SocketException;
    }
}
