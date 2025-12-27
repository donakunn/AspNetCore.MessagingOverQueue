using System.Text.Json;
using MessagingOverQueue.Abstractions.Messages;
using MessagingOverQueue.Abstractions.Publishing;
using MessagingOverQueue.Abstractions.Serialization;
using MessagingOverQueue.Persistence.Entities;
using MessagingOverQueue.Persistence.Repositories;
using Microsoft.Extensions.Logging;

namespace MessagingOverQueue.Persistence;

/// <summary>
/// Publisher that stores messages in the outbox for reliable delivery.
/// </summary>
public class OutboxPublisher : IMessagePublisher, IEventPublisher, ICommandSender
{
    private readonly IOutboxRepository _repository;
    private readonly IMessageSerializer _serializer;
    private readonly ILogger<OutboxPublisher> _logger;

    public OutboxPublisher(
        IOutboxRepository repository,
        IMessageSerializer serializer,
        ILogger<OutboxPublisher> logger)
    {
        _repository = repository;
        _serializer = serializer;
        _logger = logger;
    }

    public async Task PublishAsync<T>(T message, string? exchangeName = null, string? routingKey = null, CancellationToken cancellationToken = default) where T : IMessage
    {
        await PublishAsync(message, new PublishOptions
        {
            ExchangeName = exchangeName,
            RoutingKey = routingKey
        }, cancellationToken);
    }

    public async Task PublishAsync<T>(T message, PublishOptions options, CancellationToken cancellationToken = default) where T : IMessage
    {
        var outboxMessage = new OutboxMessage
        {
            Id = message.Id,
            MessageType = message.MessageType,
            Payload = _serializer.Serialize(message, typeof(T)),
            ExchangeName = options.ExchangeName,
            RoutingKey = options.RoutingKey ?? GetDefaultRoutingKey<T>(),
            Headers = options.Headers != null ? JsonSerializer.Serialize(options.Headers) : null,
            CreatedAt = DateTime.UtcNow,
            Status = OutboxMessageStatus.Pending,
            CorrelationId = message.CorrelationId
        };

        await _repository.AddAsync(outboxMessage, cancellationToken);
        
        _logger.LogDebug("Added message {MessageId} to outbox for exchange '{Exchange}'", 
            message.Id, options.ExchangeName ?? "(default)");
    }

    public Task PublishAsync<T>(T @event, CancellationToken cancellationToken = default) where T : IEvent
    {
        var exchangeName = GetExchangeName<T>();
        var routingKey = GetDefaultRoutingKey<T>();
        return PublishAsync(@event, exchangeName, routingKey, cancellationToken);
    }

    public Task SendAsync<T>(T command, CancellationToken cancellationToken = default) where T : ICommand
    {
        var queueName = GetQueueName<T>();
        return SendAsync(command, queueName, cancellationToken);
    }

    public Task SendAsync<T>(T command, string queueName, CancellationToken cancellationToken = default) where T : ICommand
    {
        return PublishAsync(command, string.Empty, queueName, cancellationToken);
    }

    private static string GetExchangeName<T>() where T : IMessage
    {
        var type = typeof(T);
        return $"{type.Namespace}.{type.Name}".Replace(".", "-").ToLowerInvariant();
    }

    private static string GetQueueName<T>() where T : IMessage
    {
        var type = typeof(T);
        return $"{type.Namespace}.{type.Name}".Replace(".", "-").ToLowerInvariant();
    }

    private static string GetDefaultRoutingKey<T>() where T : IMessage
    {
        var type = typeof(T);
        return type.FullName?.Replace(".", ".") ?? type.Name;
    }
}

