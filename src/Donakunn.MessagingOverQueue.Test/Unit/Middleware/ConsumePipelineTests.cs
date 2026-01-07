//using MessagingOverQueue.src.Consuming.Middleware;

//namespace MessagingOverQueue.Test.Unit.Middleware;

///// <summary>
///// Unit tests for ConsumePipeline.
///// </summary>
//public class ConsumePipelineTests
//{
//    [Fact]
//    public async Task ExecuteAsync_NoMiddleware_ExecutesTerminalHandler()
//    {
//        // Arrange
//        var executed = false;
//        var pipeline = new ConsumePipeline(
//            Enumerable.Empty<IConsumeMiddleware>(),
//            (ctx, ct) =>
//            {
//                executed = true;
//                return Task.CompletedTask;
//            });

//        var context = CreateTestContext();

//        // Act
//        await pipeline.ExecuteAsync(context, CancellationToken.None);

//        // Assert
//        Assert.True(executed);
//    }

//    [Fact]
//    public async Task ExecuteAsync_SingleMiddleware_ExecutesInOrder()
//    {
//        // Arrange
//        var executionOrder = new List<string>();
//        var middleware = new TestMiddleware("M1", executionOrder);

//        var pipeline = new ConsumePipeline(
//            [middleware],
//            (ctx, ct) =>
//            {
//                executionOrder.Add("Terminal");
//                return Task.CompletedTask;
//            });

//        // Act
//        await pipeline.ExecuteAsync(CreateTestContext(), CancellationToken.None);

//        // Assert
//        Assert.Equal(["M1-Before", "Terminal", "M1-After"], executionOrder);
//    }

//    [Fact]
//    public async Task ExecuteAsync_MultipleMiddleware_ExecutesInCorrectOrder()
//    {
//        // Arrange
//        var executionOrder = new List<string>();
//        var middlewares = new[]
//        {
//            new TestMiddleware("M1", executionOrder),
//            new TestMiddleware("M2", executionOrder),
//            new TestMiddleware("M3", executionOrder)
//        };

//        var pipeline = new ConsumePipeline(
//            middlewares,
//            (ctx, ct) =>
//            {
//                executionOrder.Add("Terminal");
//                return Task.CompletedTask;
//            });

//        // Act
//        await pipeline.ExecuteAsync(CreateTestContext(), CancellationToken.None);

//        // Assert
//        Assert.Equal([
//            "M1-Before", "M2-Before", "M3-Before",
//            "Terminal",
//            "M3-After", "M2-After", "M1-After"
//        ], executionOrder);
//    }

//    [Fact]
//    public async Task ExecuteAsync_MiddlewareCanModifyContext()
//    {
//        // Arrange
//        var middleware = new ContextModifyingMiddleware();
//        var pipeline = new ConsumePipeline(
//            [middleware],
//            (ctx, ct) => Task.CompletedTask);

//        var context = CreateTestContext();

//        // Act
//        await pipeline.ExecuteAsync(context, CancellationToken.None);

//        // Assert
//        Assert.True(context.Data.ContainsKey("Modified"));
//        Assert.Equal("true", context.Data["Modified"]);
//    }

//    [Fact]
//    public async Task ExecuteAsync_MiddlewareCanShortCircuit()
//    {
//        // Arrange
//        var terminalExecuted = false;
//        var shortCircuitMiddleware = new ShortCircuitMiddleware();

//        var pipeline = new ConsumePipeline(
//            [shortCircuitMiddleware],
//            (ctx, ct) =>
//            {
//                terminalExecuted = true;
//                return Task.CompletedTask;
//            });

//        // Act
//        await pipeline.ExecuteAsync(CreateTestContext(), CancellationToken.None);

//        // Assert
//        Assert.False(terminalExecuted);
//    }

//    [Fact]
//    public async Task ExecuteAsync_ExceptionPropagates()
//    {
//        // Arrange
//        var throwingMiddleware = new ThrowingMiddleware();
//        var pipeline = new ConsumePipeline(
//            [throwingMiddleware],
//            (ctx, ct) => Task.CompletedTask);

//        // Act & Assert
//        await Assert.ThrowsAsync<InvalidOperationException>(
//            () => pipeline.ExecuteAsync(CreateTestContext(), CancellationToken.None));
//    }

//    [Fact]
//    public async Task ExecuteAsync_CancellationIsPropagated()
//    {
//        // Arrange
//        CancellationToken? capturedToken = null;
//        var middleware = new TokenCapturingMiddleware(token => capturedToken = token);

//        using var cts = new CancellationTokenSource();
//        var pipeline = new ConsumePipeline(
//            [middleware],
//            (ctx, ct) => Task.CompletedTask);

//        // Act
//        await pipeline.ExecuteAsync(CreateTestContext(), cts.Token);

//        // Assert
//        Assert.NotNull(capturedToken);
//        Assert.Equal(cts.Token, capturedToken);
//    }

//    [Fact]
//    public async Task ExecuteAsync_ParallelExecutions_AreIndependent()
//    {
//        // Arrange
//        var executionCounts = new Dictionary<int, int>();
//        var lockObj = new object();

//        var middleware = new CountingMiddleware(executionCounts, lockObj);
//        var pipeline = new ConsumePipeline(
//            [middleware],
//            (ctx, ct) => Task.CompletedTask);

//        const int parallelCount = 50;

//        // Act
//        var tasks = Enumerable.Range(0, parallelCount)
//            .Select(i =>
//            {
//                var context = CreateTestContext();
//                context.Data["ExecutionId"] = i;
//                return pipeline.ExecuteAsync(context, CancellationToken.None);
//            });

//        await Task.WhenAll(tasks);

//        // Assert
//        Assert.Equal(parallelCount, executionCounts.Values.Sum());
//    }

//    private static ConsumeContext CreateTestContext()
//    {
//        return new ConsumeContext
//        {
//            Body = [],
//            DeliveryTag = 1,
//            Headers = new Dictionary<string, object>()
//        };
//    }

//    #region Test Middleware Implementations

//    private class TestMiddleware : IConsumeMiddleware
//    {
//        private readonly string _name;
//        private readonly List<string> _executionOrder;

//        public TestMiddleware(string name, List<string> executionOrder)
//        {
//            _name = name;
//            _executionOrder = executionOrder;
//        }

//        public async Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
//        {
//            _executionOrder.Add($"{_name}-Before");
//            await next(context, cancellationToken);
//            _executionOrder.Add($"{_name}-After");
//        }
//    }

//    private class ContextModifyingMiddleware : IConsumeMiddleware
//    {
//        public async Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
//        {
//            context.Data["Modified"] = "true";
//            await next(context, cancellationToken);
//        }
//    }

//    private class ShortCircuitMiddleware : IConsumeMiddleware
//    {
//        public Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
//        {
//            // Don't call next - short circuit
//            return Task.CompletedTask;
//        }
//    }

//    private class ThrowingMiddleware : IConsumeMiddleware
//    {
//        public Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
//        {
//            throw new InvalidOperationException("Test exception");
//        }
//    }

//    private class TokenCapturingMiddleware : IConsumeMiddleware
//    {
//        private readonly Action<CancellationToken> _capture;

//        public TokenCapturingMiddleware(Action<CancellationToken> capture)
//        {
//            _capture = capture;
//        }

//        public async Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
//        {
//            _capture(cancellationToken);
//            await next(context, cancellationToken);
//        }
//    }

//    private class CountingMiddleware : IConsumeMiddleware
//    {
//        private readonly Dictionary<int, int> _counts;
//        private readonly object _lock;

//        public CountingMiddleware(Dictionary<int, int> counts, object lockObj)
//        {
//            _counts = counts;
//            _lock = lockObj;
//        }

//        public async Task InvokeAsync(ConsumeContext context, Func<ConsumeContext, CancellationToken, Task> next, CancellationToken cancellationToken)
//        {
//            var id = (int)context.Data["ExecutionId"];
//            lock (_lock)
//            {
//                _counts[id] = _counts.GetValueOrDefault(id) + 1;
//            }
//            await next(context, cancellationToken);
//        }
//    }

//    #endregion
//}
