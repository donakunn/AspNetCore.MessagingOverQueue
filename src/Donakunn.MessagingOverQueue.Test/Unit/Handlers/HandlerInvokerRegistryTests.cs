//using MessagingOverQueue.src.Abstractions.Consuming;
//using MessagingOverQueue.src.Abstractions.Messages;
//using MessagingOverQueue.src.Consuming.Handlers;

//namespace MessagingOverQueue.Test.Unit.Handlers;

///// <summary>
///// Unit tests for HandlerInvokerRegistry thread safety and functionality.
///// </summary>
//public class HandlerInvokerRegistryTests
//{
//    [Fact]
//    public void Register_SingleInvoker_ReturnsInvoker()
//    {
//        // Arrange
//        var registry = new HandlerInvokerRegistry();
//        var factory = new HandlerInvokerFactory();
//        var invoker = factory.Create(typeof(TestMessage));

//        // Act
//        registry.Register(invoker);

//        // Assert
//        Assert.True(registry.IsRegistered(typeof(TestMessage)));
//        Assert.Same(invoker, registry.GetInvoker(typeof(TestMessage)));
//    }

//    [Fact]
//    public void GetInvoker_UnregisteredType_ReturnsNull()
//    {
//        // Arrange
//        var registry = new HandlerInvokerRegistry();

//        // Act
//        var result = registry.GetInvoker(typeof(TestMessage));

//        // Assert
//        Assert.Null(result);
//    }

//    [Fact]
//    public void IsRegistered_UnregisteredType_ReturnsFalse()
//    {
//        // Arrange
//        var registry = new HandlerInvokerRegistry();

//        // Act & Assert
//        Assert.False(registry.IsRegistered(typeof(TestMessage)));
//    }

//    [Fact]
//    public void Register_DuplicateType_DoesNotOverwrite()
//    {
//        // Arrange
//        var registry = new HandlerInvokerRegistry();
//        var factory = new HandlerInvokerFactory();
//        var invoker1 = factory.Create(typeof(TestMessage));
//        var invoker2 = factory.Create(typeof(TestMessage));

//        // Act
//        registry.Register(invoker1);
//        registry.Register(invoker2);

//        // Assert - Should return first registered invoker
//        Assert.Same(invoker1, registry.GetInvoker(typeof(TestMessage)));
//    }

//    [Fact]
//    public void GetRegisteredMessageTypes_ReturnsAllTypes()
//    {
//        // Arrange
//        var registry = new HandlerInvokerRegistry();
//        var factory = new HandlerInvokerFactory();

//        registry.Register(factory.Create(typeof(TestMessage)));
//        registry.Register(factory.Create(typeof(AnotherTestMessage)));

//        // Act
//        var types = registry.GetRegisteredMessageTypes().ToList();

//        // Assert
//        Assert.Equal(2, types.Count);
//        Assert.Contains(typeof(TestMessage), types);
//        Assert.Contains(typeof(AnotherTestMessage), types);
//    }

//    [Fact]
//    public void Register_NullInvoker_ThrowsArgumentNullException()
//    {
//        // Arrange
//        var registry = new HandlerInvokerRegistry();

//        // Act & Assert
//        Assert.Throws<ArgumentNullException>(() => registry.Register(null!));
//    }

//    [Fact]
//    public void GetInvoker_NullType_ThrowsArgumentNullException()
//    {
//        // Arrange
//        var registry = new HandlerInvokerRegistry();

//        // Act & Assert
//        Assert.Throws<ArgumentNullException>(() => registry.GetInvoker(null!));
//    }

//    [Fact]
//    public void IsRegistered_NullType_ThrowsArgumentNullException()
//    {
//        // Arrange
//        var registry = new HandlerInvokerRegistry();

//        // Act & Assert
//        Assert.Throws<ArgumentNullException>(() => registry.IsRegistered(null!));
//    }

//    [Fact]
//    public async Task ConcurrentRegistrations_AreThreadSafe()
//    {
//        // Arrange
//        var registry = new HandlerInvokerRegistry();
//        var factory = new HandlerInvokerFactory();
//        const int threadCount = 100;
//        var messageTypes = Enumerable.Range(0, threadCount)
//            .Select(_ => typeof(TestMessage))
//            .ToArray();

//        // Act - Concurrent registrations of the same type
//        var tasks = Enumerable.Range(0, threadCount)
//            .Select(_ => Task.Run(() =>
//            {
//                var invoker = factory.Create(typeof(TestMessage));
//                registry.Register(invoker);
//            }));

//        await Task.WhenAll(tasks);

//        // Assert
//        Assert.True(registry.IsRegistered(typeof(TestMessage)));
//        Assert.Single(registry.GetRegisteredMessageTypes());
//    }

//    [Fact]
//    public async Task ConcurrentReads_AreThreadSafe()
//    {
//        // Arrange
//        var registry = new HandlerInvokerRegistry();
//        var factory = new HandlerInvokerFactory();
//        var invoker = factory.Create(typeof(TestMessage));
//        registry.Register(invoker);

//        const int readerCount = 1000;
//        var results = new IHandlerInvoker?[readerCount];

//        // Act - Concurrent reads
//        var tasks = Enumerable.Range(0, readerCount)
//            .Select(i => Task.Run(() =>
//            {
//                results[i] = registry.GetInvoker(typeof(TestMessage));
//            }));

//        await Task.WhenAll(tasks);

//        // Assert - All reads should return the same invoker
//        Assert.All(results, r => Assert.Same(invoker, r));
//    }

//    [Fact]
//    public async Task ConcurrentReadsAndWrites_AreThreadSafe()
//    {
//        // Arrange
//        var registry = new HandlerInvokerRegistry();
//        var factory = new HandlerInvokerFactory();
//        const int operationCount = 500;
//        var exceptions = new List<Exception>();

//        // Act - Mix of reads and writes
//        var tasks = Enumerable.Range(0, operationCount)
//            .Select(i => Task.Run(() =>
//            {
//                try
//                {
//                    if (i % 2 == 0)
//                    {
//                        // Write
//                        var invoker = factory.Create(typeof(TestMessage));
//                        registry.Register(invoker);
//                    }
//                    else
//                    {
//                        // Read
//                        _ = registry.GetInvoker(typeof(TestMessage));
//                        _ = registry.IsRegistered(typeof(TestMessage));
//                        _ = registry.GetRegisteredMessageTypes().ToList();
//                    }
//                }
//                catch (Exception ex)
//                {
//                    lock (exceptions)
//                    {
//                        exceptions.Add(ex);
//                    }
//                }
//            }));

//        await Task.WhenAll(tasks);

//        // Assert
//        Assert.Empty(exceptions);
//        Assert.True(registry.IsRegistered(typeof(TestMessage)));
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
