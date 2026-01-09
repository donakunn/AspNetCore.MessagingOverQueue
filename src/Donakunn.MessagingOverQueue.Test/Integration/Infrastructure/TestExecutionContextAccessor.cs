namespace MessagingOverQueue.Test.Integration.Infrastructure;

/// <summary>
/// Provides access to the current test execution context.
/// Uses AsyncLocal to ensure thread-safe isolation between parallel test executions.
/// </summary>
public static class TestExecutionContextAccessor
{
    private static readonly AsyncLocal<TestExecutionContext?> _current = new();

    /// <summary>
    /// Gets or sets the current test execution context for this async flow.
    /// </summary>
    public static TestExecutionContext? Current
    {
        get => _current.Value;
        set => _current.Value = value;
    }

    /// <summary>
    /// Gets the current context, throwing if not set.
    /// </summary>
    public static TestExecutionContext GetRequired()
    {
        return Current ?? throw new InvalidOperationException(
            "TestExecutionContext is not set. Ensure the test fixture properly initializes the context.");
    }
}
