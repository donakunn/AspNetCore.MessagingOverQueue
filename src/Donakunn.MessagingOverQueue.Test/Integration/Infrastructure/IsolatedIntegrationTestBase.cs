namespace MessagingOverQueue.Test.Integration.Infrastructure;

/// <summary>
/// Base class for integration tests that provides isolated test execution context.
/// Ensures each test has its own context, preventing interference between parallel test executions.
/// </summary>
public abstract class IsolatedIntegrationTestBase : IDisposable
{
    private readonly TestExecutionContext _testContext;
    private bool _disposed;

    protected IsolatedIntegrationTestBase()
    {
        // Create a new context for this test instance
        _testContext = new TestExecutionContext();
        
        // Set it as the current context for this async flow
        TestExecutionContextAccessor.Current = _testContext;
    }

    /// <summary>
    /// Gets the test execution context for this test.
    /// </summary>
    protected TestExecutionContext TestContext => _testContext;

    /// <summary>
    /// Resets all handler state in the test context.
    /// Call this in test setup or between test phases if needed.
    /// </summary>
    protected void ResetHandlerState()
    {
        _testContext.Reset();
    }

    public virtual void Dispose()
    {
        if (_disposed)
            return;

        // Clean up the context
        _testContext.Reset();
        TestExecutionContextAccessor.Current = null;

        _disposed = true;
        GC.SuppressFinalize(this);
    }
}
