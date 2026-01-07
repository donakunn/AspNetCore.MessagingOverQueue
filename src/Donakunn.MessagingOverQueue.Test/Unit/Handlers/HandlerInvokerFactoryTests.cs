//using MessagingOverQueue.src.Abstractions.Messages;
//using MessagingOverQueue.src.Consuming.Handlers;

//namespace MessagingOverQueue.Test.Unit.Handlers;

///// <summary>
///// Unit tests for HandlerInvokerFactory.
///// </summary>
//public class HandlerInvokerFactoryTests
//{
//    [Fact]
//    public void Create_ValidMessageType_ReturnsInvoker()
//    {
//        // Arrange
//        var factory = new HandlerInvokerFactory();

//        // Act
//        var invoker = factory.Create(typeof(TestMessage));

//        // Assert
//        Assert.NotNull(invoker);
//        Assert.Equal(typeof(TestMessage), invoker.MessageType);
//    }

//    [Fact]
//    public void Create_NullType_ThrowsArgumentNullException()
//    {
//        // Arrange
//        var factory = new HandlerInvokerFactory();

//        // Act & Assert
//        Assert.Throws<ArgumentNullException>(() => factory.Create(null!));
//    }

//    [Fact]
//    public void Create_NonMessageType_ThrowsArgumentException()
//    {
//        // Arrange
//        var factory = new HandlerInvokerFactory();

//        // Act & Assert
//        var ex = Assert.Throws<ArgumentException>(() => factory.Create(typeof(string)));
//        Assert.Contains("does not implement IMessage", ex.Message);
//    }

//    [Fact]
//    public void Create_MultipleInvocations_ReturnsDifferentInstances()
//    {
//        // Arrange
//        var factory = new HandlerInvokerFactory();

//        // Act
//        var invoker1 = factory.Create(typeof(TestMessage));
//        var invoker2 = factory.Create(typeof(TestMessage));

//        // Assert
//        Assert.NotSame(invoker1, invoker2);
//        Assert.Equal(invoker1.MessageType, invoker2.MessageType);
//    }

//    [Fact]
//    public void Create_DifferentMessageTypes_ReturnsDifferentInvokerTypes()
//    {
//        // Arrange
//        var factory = new HandlerInvokerFactory();

//        // Act
//        var invoker1 = factory.Create(typeof(TestMessage));
//        var invoker2 = factory.Create(typeof(AnotherTestMessage));

//        // Assert
//        Assert.NotEqual(invoker1.MessageType, invoker2.MessageType);
//        Assert.Equal(typeof(TestMessage), invoker1.MessageType);
//        Assert.Equal(typeof(AnotherTestMessage), invoker2.MessageType);
//    }

//    [Fact]
//    public async Task Create_IsThreadSafe()
//    {
//        // Arrange
//        var factory = new HandlerInvokerFactory();
//        const int threadCount = 100;
//        var invokers = new IHandlerInvoker[threadCount];

//        // Act
//        var tasks = Enumerable.Range(0, threadCount)
//            .Select(i => Task.Run(() =>
//            {
//                invokers[i] = factory.Create(typeof(TestMessage));
//            }));

//        await Task.WhenAll(tasks);

//        // Assert
//        Assert.All(invokers, inv => Assert.NotNull(inv));
//        Assert.All(invokers, inv => Assert.Equal(typeof(TestMessage), inv.MessageType));
//    }

//    // Test message types
//    private class TestMessage : IMessage
//    {
//        public Guid Id { get; init; } = Guid.NewGuid();
//        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
//        public string? CorrelationId { get; init; }
//        public string? CausationId { get; init; }
//        public string MessageType => GetType().FullName!;
//    }

//    private class AnotherTestMessage : IMessage
//    {
//        public Guid Id { get; init; } = Guid.NewGuid();
//        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
//        public string? CorrelationId { get; init; }
//        public string? CausationId { get; init; }
//        public string MessageType => GetType().FullName!;
//    }
//}
