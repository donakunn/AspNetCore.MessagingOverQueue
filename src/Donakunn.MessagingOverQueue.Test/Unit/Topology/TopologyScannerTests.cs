//using MessagingOverQueue.src.Abstractions.Consuming;
//using MessagingOverQueue.src.Abstractions.Messages;
//using MessagingOverQueue.src.Topology;
//using MessagingOverQueue.src.Topology.Abstractions;
//using MessagingOverQueue.src.Topology.Attributes;
//using System.Reflection;

//namespace MessagingOverQueue.Test.Unit.Topology;

///// <summary>
///// Unit tests for TopologyScanner.
///// </summary>
//public class TopologyScannerTests
//{
//    private readonly TopologyScanner _scanner = new();

//    [Fact]
//    public void ScanForMessageTypes_FindsEventTypes()
//    {
//        // Arrange
//        var assemblies = new[] { typeof(TestEvent).Assembly };

//        // Act
//        var messageTypes = _scanner.ScanForMessageTypes(assemblies);

//        // Assert
//        Assert.Contains(messageTypes, m => m.MessageType == typeof(TestEvent));
//    }

//    [Fact]
//    public void ScanForMessageTypes_FindsCommandTypes()
//    {
//        // Arrange
//        var assemblies = new[] { typeof(TestCommand).Assembly };

//        // Act
//        var messageTypes = _scanner.ScanForMessageTypes(assemblies);

//        // Assert
//        var commandType = messageTypes.FirstOrDefault(m => m.MessageType == typeof(TestCommand));
//        Assert.NotNull(commandType);
//        Assert.True(commandType.IsCommand);
//        Assert.False(commandType.IsEvent);
//    }

//    [Fact]
//    public void ScanForMessageTypes_CorrectlyIdentifiesEventVsCommand()
//    {
//        // Arrange
//        var assemblies = new[] { typeof(TestEvent).Assembly };

//        // Act
//        var messageTypes = _scanner.ScanForMessageTypes(assemblies);

//        // Assert
//        var eventType = messageTypes.FirstOrDefault(m => m.MessageType == typeof(TestEvent));
//        var commandType = messageTypes.FirstOrDefault(m => m.MessageType == typeof(TestCommand));

//        Assert.NotNull(eventType);
//        Assert.NotNull(commandType);
//        Assert.True(eventType.IsEvent);
//        Assert.False(eventType.IsCommand);
//        Assert.True(commandType.IsCommand);
//        Assert.False(commandType.IsEvent);
//    }

//    [Fact]
//    public void ScanForMessageTypes_RespectsAutoDiscoverAttribute()
//    {
//        // Arrange
//        var assemblies = new[] { typeof(NonDiscoverableMessage).Assembly };

//        // Act
//        var messageTypes = _scanner.ScanForMessageTypes(assemblies);

//        // Assert - Should not contain the non-discoverable message
//        Assert.DoesNotContain(messageTypes, m => m.MessageType == typeof(NonDiscoverableMessage));
//    }

//    [Fact]
//    public void ScanForMessageTypes_CollectsAttributes()
//    {
//        // Arrange
//        var assemblies = new[] { typeof(AttributedMessage).Assembly };

//        // Act
//        var messageTypes = _scanner.ScanForMessageTypes(assemblies);

//        // Assert
//        var attributedType = messageTypes.FirstOrDefault(m => m.MessageType == typeof(AttributedMessage));
//        Assert.NotNull(attributedType);
//        Assert.Contains(attributedType.Attributes, a => a is QueueAttribute);
//        Assert.Contains(attributedType.Attributes, a => a is ExchangeAttribute);
//    }

//    [Fact]
//    public void ScanForMessageTypes_NullAssemblies_ThrowsArgumentNullException()
//    {
//        // Act & Assert
//        Assert.Throws<ArgumentNullException>(() => _scanner.ScanForMessageTypes(null!));
//    }

//    [Fact]
//    public void ScanForMessageTypes_EmptyAssemblies_ReturnsEmpty()
//    {
//        // Act
//        var messageTypes = _scanner.ScanForMessageTypes();

//        // Assert
//        Assert.Empty(messageTypes);
//    }

//    [Fact]
//    public void ScanForHandlers_FindsHandlerTypes()
//    {
//        // Arrange
//        var assemblies = new[] { typeof(TestEventHandler).Assembly };

//        // Act
//        var handlers = _scanner.ScanForHandlers(assemblies);

//        // Assert
//        Assert.Contains(handlers, h => h.HandlerType == typeof(TestEventHandler));
//    }

//    [Fact]
//    public void ScanForHandlers_IdentifiesMessageType()
//    {
//        // Arrange
//        var assemblies = new[] { typeof(TestEventHandler).Assembly };

//        // Act
//        var handlers = _scanner.ScanForHandlers(assemblies);

//        // Assert
//        var handler = handlers.FirstOrDefault(h => h.HandlerType == typeof(TestEventHandler));
//        Assert.NotNull(handler);
//        Assert.Equal(typeof(TestEvent), handler.MessageType);
//    }

//    [Fact]
//    public void ScanForHandlers_IgnoresAbstractHandlers()
//    {
//        // Arrange
//        var assemblies = new[] { typeof(AbstractHandler).Assembly };

//        // Act
//        var handlers = _scanner.ScanForHandlers(assemblies);

//        // Assert
//        Assert.DoesNotContain(handlers, h => h.HandlerType == typeof(AbstractHandler));
//    }

//    [Fact]
//    public void ScanForHandlerTopology_ReturnsCompleteInfo()
//    {
//        // Arrange
//        var assemblies = new[] { typeof(TestEventHandler).Assembly };

//        // Act
//        var topologyInfos = _scanner.ScanForHandlerTopology(assemblies);

//        // Assert
//        var info = topologyInfos.FirstOrDefault(t => t.HandlerType == typeof(TestEventHandler));
//        Assert.NotNull(info);
//        Assert.Equal(typeof(TestEvent), info.MessageType);
//        Assert.True(info.IsEvent);
//        Assert.False(info.IsCommand);
//    }

//    [Fact]
//    public void ScanForHandlerTopology_IncludesConsumerQueueConfig()
//    {
//        // Arrange
//        var assemblies = new[] { typeof(HandlerWithConsumerQueue).Assembly };

//        // Act
//        var topologyInfos = _scanner.ScanForHandlerTopology(assemblies);

//        // Assert
//        var info = topologyInfos.FirstOrDefault(t => t.HandlerType == typeof(HandlerWithConsumerQueue));
//        Assert.NotNull(info);
//        Assert.NotNull(info.ConsumerQueueConfig);
//        Assert.Equal("custom-test-queue", info.ConsumerQueueConfig.QueueName);
//        Assert.Equal(50, info.ConsumerQueueConfig.PrefetchCount);
//    }

//    [Fact]
//    public void ScanForHandlerTopology_ExcludesMessageWithAutoDiscoverFalse()
//    {
//        // Arrange
//        var assemblies = new[] { typeof(NonDiscoverableMessageHandler).Assembly };

//        // Act
//        var topologyInfos = _scanner.ScanForHandlerTopology(assemblies);

//        // Assert
//        Assert.DoesNotContain(topologyInfos, t => t.MessageType == typeof(NonDiscoverableMessage));
//    }

//    [Fact]
//    public async Task ScanForHandlers_IsThreadSafe()
//    {
//        // Arrange
//        var assemblies = new[] { typeof(TestEventHandler).Assembly };
//        const int iterations = 50;
//        var results = new List<IReadOnlyCollection<HandlerTypeInfo>>();
//        var lockObj = new object();

//        // Act
//        var tasks = Enumerable.Range(0, iterations)
//            .Select(_ => Task.Run(() =>
//            {
//                var handlers = _scanner.ScanForHandlers(assemblies);
//                lock (lockObj)
//                {
//                    results.Add(handlers);
//                }
//            }));

//        await Task.WhenAll(tasks);

//        // Assert - All results should be equivalent
//        Assert.Equal(iterations, results.Count);
//        var firstCount = results[0].Count;
//        Assert.All(results, r => Assert.Equal(firstCount, r.Count));
//    }

//    #region Test Types

//    public class TestEvent : Event
//    {
//        public string Data { get; set; } = string.Empty;
//    }

//    public class TestCommand : Command
//    {
//        public string Action { get; set; } = string.Empty;
//    }

//    [Message(AutoDiscover = false)]
//    public class NonDiscoverableMessage : Event
//    {
//    }

//    [Queue("test-queue")]
//    [Exchange("test-exchange")]
//    public class AttributedMessage : Event
//    {
//    }

//    public class TestEventHandler : IMessageHandler<TestEvent>
//    {
//        public Task HandleAsync(TestEvent message, IMessageContext context, CancellationToken cancellationToken)
//            => Task.CompletedTask;
//    }

//    [ConsumerQueue(Name = "custom-test-queue", PrefetchCount = 50)]
//    public class HandlerWithConsumerQueue : IMessageHandler<TestEvent>
//    {
//        public Task HandleAsync(TestEvent message, IMessageContext context, CancellationToken cancellationToken)
//            => Task.CompletedTask;
//    }

//    public class NonDiscoverableMessageHandler : IMessageHandler<NonDiscoverableMessage>
//    {
//        public Task HandleAsync(NonDiscoverableMessage message, IMessageContext context, CancellationToken cancellationToken)
//            => Task.CompletedTask;
//    }

//    public abstract class AbstractHandler : IMessageHandler<TestEvent>
//    {
//        public abstract Task HandleAsync(TestEvent message, IMessageContext context, CancellationToken cancellationToken);
//    }

//    #endregion
//}
