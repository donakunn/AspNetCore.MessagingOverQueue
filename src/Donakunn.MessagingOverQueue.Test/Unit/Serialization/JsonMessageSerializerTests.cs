//using MessagingOverQueue.src.Abstractions.Messages;
//using MessagingOverQueue.src.Abstractions.Serialization;
//using System.Text.Json;

//namespace MessagingOverQueue.Test.Unit.Serialization;

///// <summary>
///// Unit tests for JsonMessageSerializer.
///// </summary>
//public class JsonMessageSerializerTests
//{
//    private readonly JsonMessageSerializer _serializer = new();

//    [Fact]
//    public void Serialize_SimpleMessage_ReturnsValidJson()
//    {
//        // Arrange
//        var message = new TestMessage { Value = "Hello" };

//        // Act
//        var bytes = _serializer.Serialize(message);
//        var json = System.Text.Encoding.UTF8.GetString(bytes);

//        // Assert
//        Assert.NotEmpty(bytes);
//        Assert.Contains("value", json);
//        Assert.Contains("Hello", json);
//    }

//    [Fact]
//    public void Deserialize_ValidJson_ReturnsMessage()
//    {
//        // Arrange
//        var message = new TestMessage { Value = "Test" };
//        var bytes = _serializer.Serialize(message);

//        // Act
//        var result = _serializer.Deserialize<TestMessage>(bytes);

//        // Assert
//        Assert.NotNull(result);
//        Assert.Equal("Test", result.Value);
//        Assert.Equal(message.Id, result.Id);
//    }

//    [Fact]
//    public void SerializeAndDeserialize_ComplexMessage_PreservesAllProperties()
//    {
//        // Arrange
//        var originalId = Guid.NewGuid();
//        var message = new ComplexTestMessage
//        {
//            Name = "Complex",
//            Count = 42,
//            Amount = 99.99m,
//            IsActive = true,
//            Tags = ["tag1", "tag2"],
//            Metadata = new Dictionary<string, object>
//            {
//                ["key1"] = "value1",
//                ["nested"] = new { inner = "data" }
//            }
//        };
//        typeof(ComplexTestMessage).GetProperty("Id")!.SetValue(message, originalId);

//        // Act
//        var bytes = _serializer.Serialize(message);
//        var result = _serializer.Deserialize<ComplexTestMessage>(bytes);

//        // Assert
//        Assert.NotNull(result);
//        Assert.Equal("Complex", result.Name);
//        Assert.Equal(42, result.Count);
//        Assert.Equal(99.99m, result.Amount);
//        Assert.True(result.IsActive);
//        Assert.Equal(2, result.Tags.Count);
//        Assert.Equal(originalId, result.Id);
//    }

//    [Fact]
//    public void ContentType_ReturnsApplicationJson()
//    {
//        // Assert
//        Assert.Equal("application/json", _serializer.ContentType);
//    }

//    [Fact]
//    public void Serialize_WithNullProperties_OmitsNullValues()
//    {
//        // Arrange
//        var message = new TestMessage { Value = "Test" };

//        // Act
//        var bytes = _serializer.Serialize(message);
//        var json = System.Text.Encoding.UTF8.GetString(bytes);

//        // Assert - Should not contain "correlationId" since it's null
//        Assert.DoesNotContain("\"correlationId\":null", json);
//    }

//    [Fact]
//    public void Deserialize_WithMissingOptionalProperties_SetsDefaults()
//    {
//        // Arrange
//        var json = """{"id":"00000000-0000-0000-0000-000000000001","timestamp":"2024-01-01T00:00:00Z","value":"Test","messageType":"test"}""";
//        var bytes = System.Text.Encoding.UTF8.GetBytes(json);

//        // Act
//        var result = _serializer.Deserialize<TestMessage>(bytes);

//        // Assert
//        Assert.NotNull(result);
//        Assert.Equal("Test", result.Value);
//        Assert.Null(result.CorrelationId);
//    }

//    [Fact]
//    public void SerializeWithType_PreservesMessageType()
//    {
//        // Arrange
//        var message = new TestMessage { Value = "Typed" };

//        // Act
//        var bytes = _serializer.Serialize(message, typeof(TestMessage));
//        var result = _serializer.Deserialize(bytes, typeof(TestMessage)) as TestMessage;

//        // Assert
//        Assert.NotNull(result);
//        Assert.Equal("Typed", result.Value);
//    }

//    [Fact]
//    public async Task Serialize_IsThreadSafe()
//    {
//        // Arrange
//        const int iterations = 100;
//        var messages = Enumerable.Range(0, iterations)
//            .Select(i => new TestMessage { Value = $"Message-{i}" })
//            .ToList();
//        var results = new byte[iterations][];

//        // Act
//        var tasks = messages.Select((msg, i) => Task.Run(() =>
//        {
//            results[i] = _serializer.Serialize(msg);
//        }));

//        await Task.WhenAll(tasks);

//        // Assert
//        Assert.All(results, r => Assert.NotEmpty(r));
//    }

//    [Fact]
//    public async Task Deserialize_IsThreadSafe()
//    {
//        // Arrange
//        const int iterations = 100;
//        var originalMessage = new TestMessage { Value = "Concurrent" };
//        var bytes = _serializer.Serialize(originalMessage);
//        var results = new TestMessage?[iterations];

//        // Act
//        var tasks = Enumerable.Range(0, iterations)
//            .Select(i => Task.Run(() =>
//            {
//                results[i] = _serializer.Deserialize<TestMessage>(bytes);
//            }));

//        await Task.WhenAll(tasks);

//        // Assert
//        Assert.All(results, r =>
//        {
//            Assert.NotNull(r);
//            Assert.Equal("Concurrent", r!.Value);
//        });
//    }

//    [Fact]
//    public void Deserialize_InvalidJson_ReturnsNull()
//    {
//        // Arrange
//        var invalidJson = System.Text.Encoding.UTF8.GetBytes("not valid json {");

//        // Act & Assert
//        Assert.Throws<JsonException>(() => _serializer.Deserialize<TestMessage>(invalidJson));
//    }

//    [Fact]
//    public void Serialize_EmptyCollection_SerializesCorrectly()
//    {
//        // Arrange
//        var message = new ComplexTestMessage
//        {
//            Name = "Empty",
//            Tags = [],
//            Metadata = new Dictionary<string, object>()
//        };

//        // Act
//        var bytes = _serializer.Serialize(message);
//        var result = _serializer.Deserialize<ComplexTestMessage>(bytes);

//        // Assert
//        Assert.NotNull(result);
//        Assert.Empty(result.Tags);
//    }

//    // Test messages
//    private class TestMessage : IMessage
//    {
//        public Guid Id { get; init; } = Guid.NewGuid();
//        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
//        public string? CorrelationId { get; init; }
//        public string? CausationId { get; init; }
//        public string MessageType => GetType().FullName!;
//        public string Value { get; set; } = string.Empty;
//    }

//    private class ComplexTestMessage : IMessage
//    {
//        public Guid Id { get; init; } = Guid.NewGuid();
//        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
//        public string? CorrelationId { get; init; }
//        public string? CausationId { get; init; }
//        public string MessageType => GetType().FullName!;

//        public string Name { get; set; } = string.Empty;
//        public int Count { get; set; }
//        public decimal Amount { get; set; }
//        public bool IsActive { get; set; }
//        public List<string> Tags { get; set; } = [];
//        public Dictionary<string, object> Metadata { get; set; } = new();
//    }
//}
