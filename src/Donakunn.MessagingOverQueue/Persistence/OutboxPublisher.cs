using MessagingOverQueue.src.Abstractions.Messages;
using MessagingOverQueue.src.Abstractions.Publishing;
using MessagingOverQueue.src.Abstractions.Serialization;
using MessagingOverQueue.src.Persistence.Entities;
using MessagingOverQueue.src.Persistence.Repositories;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace MessagingOverQueue.src.Persistence;

/// <summary>
/// Publisher that stores messages in the outbox for reliable delivery.
/// </summary>
public class OutboxPublisher(
    IOutboxRepository repository,
    IMessageSerializer serializer,
    ILogger<OutboxPublisher> logger) : IMessagePublisher, IEventPublisher, ICommandSender
{
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
            Payload = serializer.Serialize(message),
            ExchangeName = options.ExchangeName,
            RoutingKey = options.RoutingKey ?? GetDefaultRoutingKey<T>(),
            Headers = options.Headers != null ? JsonSerializer.Serialize(options.Headers) : null,
            CreatedAt = DateTime.UtcNow,
            Status = OutboxMessageStatus.Pending,
            CorrelationId = message.CorrelationId
        };

        await repository.AddAsync(outboxMessage, cancellationToken);

        logger.LogDebug("Added message {MessageId} to outbox for exchange '{Exchange}'",
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

