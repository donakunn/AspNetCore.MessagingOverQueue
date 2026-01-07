using System.ComponentModel.DataAnnotations;

namespace MessagingOverQueue.src.Resilience.CircuitBreaker;

/// <summary>
/// Configuration for circuit breaker with validation.
/// </summary>
public class CircuitBreakerOptions
{
    private int _failureThreshold = 5;
    private TimeSpan _durationOfBreak = TimeSpan.FromSeconds(30);
    private TimeSpan _samplingDuration = TimeSpan.FromMinutes(1);
    private int _minimumThroughput = 10;
    private double _failureRateThreshold = 0.5;

    /// <summary>
    /// Number of failures before opening the circuit.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int FailureThreshold
    {
        get => _failureThreshold;
        set => _failureThreshold = value > 0 ? value : throw new ArgumentOutOfRangeException(nameof(FailureThreshold), "Must be greater than 0");
    }

    /// <summary>
    /// Duration the circuit stays open.
    /// </summary>
    public TimeSpan DurationOfBreak
    {
        get => _durationOfBreak;
        set => _durationOfBreak = value > TimeSpan.Zero ? value : throw new ArgumentOutOfRangeException(nameof(DurationOfBreak), "Must be greater than zero");
    }

    /// <summary>
    /// Sampling duration for failure counting.
    /// </summary>
    public TimeSpan SamplingDuration
    {
        get => _samplingDuration;
        set => _samplingDuration = value > TimeSpan.Zero ? value : throw new ArgumentOutOfRangeException(nameof(SamplingDuration), "Must be greater than zero");
    }

    /// <summary>
    /// Minimum throughput before circuit can open.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MinimumThroughput
    {
        get => _minimumThroughput;
        set => _minimumThroughput = value > 0 ? value : throw new ArgumentOutOfRangeException(nameof(MinimumThroughput), "Must be greater than 0");
    }

    /// <summary>
    /// Failure rate threshold (0.0 to 1.0).
    /// </summary>
    [Range(0.0, 1.0)]
    public double FailureRateThreshold
    {
        get => _failureRateThreshold;
        set => _failureRateThreshold = value is >= 0.0 and <= 1.0 ? value : throw new ArgumentOutOfRangeException(nameof(FailureRateThreshold), "Must be between 0.0 and 1.0");
    }

    /// <summary>
    /// Validates all options.
    /// </summary>
    public void Validate()
    {
        if (FailureThreshold <= 0)
            throw new ValidationException("FailureThreshold must be greater than 0");
        if (DurationOfBreak <= TimeSpan.Zero)
            throw new ValidationException("DurationOfBreak must be greater than zero");
        if (SamplingDuration <= TimeSpan.Zero)
            throw new ValidationException("SamplingDuration must be greater than zero");
        if (MinimumThroughput <= 0)
            throw new ValidationException("MinimumThroughput must be greater than 0");
        if (FailureRateThreshold is < 0.0 or > 1.0)
            throw new ValidationException("FailureRateThreshold must be between 0.0 and 1.0");
    }
}
