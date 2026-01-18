using Donakunn.MessagingOverQueue.Consuming.Middleware;
using Donakunn.MessagingOverQueue.Abstractions.Consuming;

namespace MessagingOverQueue.Test.Unit.Middleware;

/// <summary>
/// Unit tests for middleware ordering in the ConsumePipeline.
/// </summary>
public class MiddlewareOrderingTests
{
    [Fact]
    public async Task ExecuteAsync_OrderedMiddleware_ExecutesInOrderByOrderProperty()
    {
        // Arrange
        var executionOrder = new List<int>();
        var middlewares = new IConsumeMiddleware[]
        {
            new OrderedTestMiddleware(300, executionOrder),
            new OrderedTestMiddleware(100, executionOrder),
            new OrderedTestMiddleware(200, executionOrder)
        };

        var pipeline = new ConsumePipeline(
            middlewares,
            (ctx, ct) =>
            {
                executionOrder.Add(-1); // Terminal handler marker
                return Task.CompletedTask;
            });

        var context = CreateTestContext();

        // Act
        await pipeline.ExecuteAsync(context, CancellationToken.None);

        // Assert - should execute in order: 100, 200, 300, then terminal (-1)
        Assert.Equal([100, 200, 300, -1], executionOrder);
    }

    [Fact]
    public async Task ExecuteAsync_MixedOrderedAndUnordered_OrderedFirst()
    {
        // Arrange
        var executionOrder = new List<string>();
        var middlewares = new IConsumeMiddleware[]
        {
            new UnorderedTestMiddleware("Unordered1", executionOrder),
            new NamedOrderedMiddleware("Ordered100", 100, executionOrder),
            new UnorderedTestMiddleware("Unordered2", executionOrder)
        };

        var pipeline = new ConsumePipeline(
            middlewares,
            (ctx, ct) =>
            {
                executionOrder.Add("Terminal");
                return Task.CompletedTask;
            });

        var context = CreateTestContext();

        // Act
        await pipeline.ExecuteAsync(context, CancellationToken.None);

        // Assert - ordered (100) first, then unordered (default 1000)
        // Execution: Ordered100 -> Unordered1 -> Unordered2 -> Terminal
        Assert.Equal("Ordered100", executionOrder[0]);
        Assert.Contains("Unordered1", executionOrder);
        Assert.Contains("Unordered2", executionOrder);
        Assert.Equal("Terminal", executionOrder.Last());
    }

    [Fact]
    public async Task ExecuteAsync_MiddlewareOrderConstants_CorrectOrdering()
    {
        // Arrange
        var executionOrder = new List<string>();
        var middlewares = new IConsumeMiddleware[]
        {
            new NamedOrderedMiddleware("Deserialization", MiddlewareOrder.Deserialization, executionOrder),
            new NamedOrderedMiddleware("CircuitBreaker", MiddlewareOrder.CircuitBreaker, executionOrder),
            new NamedOrderedMiddleware("Retry", MiddlewareOrder.Retry, executionOrder),
            new NamedOrderedMiddleware("Timeout", MiddlewareOrder.Timeout, executionOrder),
            new NamedOrderedMiddleware("Logging", MiddlewareOrder.Logging, executionOrder),
            new NamedOrderedMiddleware("Idempotency", MiddlewareOrder.Idempotency, executionOrder)
        };

        var pipeline = new ConsumePipeline(
            middlewares,
            (ctx, ct) =>
            {
                executionOrder.Add("Terminal");
                return Task.CompletedTask;
            });

        var context = CreateTestContext();

        // Act
        await pipeline.ExecuteAsync(context, CancellationToken.None);

        // Assert - should execute in order: CircuitBreaker -> Retry -> Timeout -> Logging -> Idempotency -> Deserialization -> Terminal
        var expectedOrder = new[]
        {
            "CircuitBreaker",
            "Retry",
            "Timeout",
            "Logging",
            "Idempotency",
            "Deserialization",
            "Terminal"
        };

        Assert.Equal(expectedOrder, executionOrder);
    }

    [Fact]
    public async Task ExecuteAsync_PipelineBuiltOnce_ThreadSafe()
    {
        // Arrange
        var executionCount = 0;
        var middlewares = new IConsumeMiddleware[]
        {
            new CountingMiddleware(() => Interlocked.Increment(ref executionCount))
        };

        var pipeline = new ConsumePipeline(
            middlewares,
            (ctx, ct) => Task.CompletedTask);

        // Act - Execute in parallel
        var tasks = Enumerable.Range(0, 100)
            .Select(_ => pipeline.ExecuteAsync(CreateTestContext(), CancellationToken.None));

        await Task.WhenAll(tasks);

        // Assert - Each execution should increment the count
        Assert.Equal(100, executionCount);
    }

    [Fact]
    public void MiddlewareOrder_Constants_HaveCorrectRelativeOrdering()
    {
        // Assert that the constants have the expected relative ordering
        Assert.True(MiddlewareOrder.CircuitBreaker < MiddlewareOrder.Retry);
        Assert.True(MiddlewareOrder.Retry < MiddlewareOrder.Timeout);
        Assert.True(MiddlewareOrder.Timeout < MiddlewareOrder.Logging);
        Assert.True(MiddlewareOrder.Logging < MiddlewareOrder.Idempotency);
        Assert.True(MiddlewareOrder.Idempotency < MiddlewareOrder.Deserialization);
        Assert.True(MiddlewareOrder.Deserialization < MiddlewareOrder.Default);
    }

    private static ConsumeContext CreateTestContext()
    {
        return new ConsumeContext
        {
            Body = [],
            DeliveryTag = 1,
            MessageContext = new FakeMessageContext(),
            Headers = new Dictionary<string, object>()
        };
    }

    #region Test Middleware Implementations

    private class OrderedTestMiddleware(int order, List<int> executionOrder) : IOrderedConsumeMiddleware
    {
        public int Order => order;

        public Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
        {
            executionOrder.Add(order);
            return next(context, cancellationToken);
        }
    }

    private class UnorderedTestMiddleware(string name, List<string> executionOrder) : IConsumeMiddleware
    {
        public Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
        {
            executionOrder.Add(name);
            return next(context, cancellationToken);
        }
    }

    private class NamedOrderedMiddleware(string name, int order, List<string> executionOrder) : IOrderedConsumeMiddleware
    {
        public int Order => order;

        public Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
        {
            executionOrder.Add(name);
            return next(context, cancellationToken);
        }
    }

    private class CountingMiddleware(Action increment) : IConsumeMiddleware
    {
        public Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
        {
            increment();
            return next(context, cancellationToken);
        }
    }

    private class FakeMessageContext : IMessageContext
    {
        private readonly Dictionary<string, object?> _data = new();

        public Guid MessageId => Guid.NewGuid();
        public string? CorrelationId => null;
        public string? CausationId => null;
        public string QueueName => "test-queue";
        public string? ExchangeName => "test-exchange";
        public string? RoutingKey => "test-routing-key";
        public IReadOnlyDictionary<string, object> Headers => new Dictionary<string, object>();
        public int DeliveryCount => 1;
        public DateTime ReceivedAt => DateTime.UtcNow;

        public void SetData<T>(string key, T value) => _data[key] = value;
        public T? GetData<T>(string key) => _data.TryGetValue(key, out var value) ? (T?)value : default;
    }

    #endregion
}
