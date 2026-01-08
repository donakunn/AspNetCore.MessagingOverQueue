using Donakunn.MessagingOverQueue.Abstractions.Messages;
using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.Abstractions.Serialization;
using Donakunn.MessagingOverQueue.Persistence.Entities;
using Donakunn.MessagingOverQueue.Persistence.Repositories;
using Donakunn.MessagingOverQueue.Topology;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace Donakunn.MessagingOverQueue.Persistence;

/// <summary>
/// Publisher that stores messages in the outbox for reliable delivery.
/// </summary>
public class OutboxPublisher(
    IOutboxRepository repository,
    IMessageSerializer serializer,
    IMessageRoutingResolver routingResolver,
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
        // Use routing resolver for defaults if not explicitly specified
        var exchangeName = options.ExchangeName ?? routingResolver.GetExchangeName<T>();
        var routingKey = options.RoutingKey ?? routingResolver.GetRoutingKey<T>();

        var outboxMessage = new OutboxMessage
        {
            Id = message.Id,
            MessageType = message.MessageType,
            Payload = serializer.Serialize(message),
            ExchangeName = exchangeName,
            RoutingKey = routingKey,
            Headers = options.Headers != null ? JsonSerializer.Serialize(options.Headers) : null,
            CreatedAt = DateTime.UtcNow,
            Status = OutboxMessageStatus.Pending,
            CorrelationId = message.CorrelationId
        };

        await repository.AddAsync(outboxMessage, cancellationToken);

        logger.LogDebug("Added message {MessageId} to outbox for exchange '{Exchange}' with routing key '{RoutingKey}'",
            message.Id, exchangeName, routingKey);
    }

    public Task PublishAsync<T>(T @event, CancellationToken cancellationToken = default) where T : IEvent
    {
        var exchangeName = routingResolver.GetExchangeName<T>();
        var routingKey = routingResolver.GetRoutingKey<T>();
        return PublishAsync(@event, exchangeName, routingKey, cancellationToken);
    }

    public Task SendAsync<T>(T command, CancellationToken cancellationToken = default) where T : ICommand
    {
        var queueName = routingResolver.GetQueueName<T>();
        return SendAsync(command, queueName, cancellationToken);
    }

    public Task SendAsync<T>(T command, string queueName, CancellationToken cancellationToken = default) where T : ICommand
    {
        return PublishAsync(command, string.Empty, queueName, cancellationToken);
    }
}

