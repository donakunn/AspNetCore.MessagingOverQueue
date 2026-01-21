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
/// Use this when you need transactional consistency with your database operations.
/// </summary>
public sealed class OutboxPublisher : IMessagePublisher, IEventPublisher, ICommandSender
{
    private readonly IOutboxRepository _repository;
    private readonly IMessageSerializer _serializer;
    private readonly IMessageRoutingResolver _routingResolver;
    private readonly ILogger<OutboxPublisher> _logger;

    public OutboxPublisher(
        IOutboxRepository repository,
        IMessageSerializer serializer,
        IMessageRoutingResolver routingResolver,
        ILogger<OutboxPublisher> logger)
    {
        _repository = repository;
        _serializer = serializer;
        _routingResolver = routingResolver;
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
        // Use routing resolver for defaults if not explicitly specified
        var exchangeName = options.ExchangeName ?? _routingResolver.GetExchangeName<T>();
        var routingKey = options.RoutingKey ?? _routingResolver.GetRoutingKey<T>();
        var queueName = _routingResolver.GetQueueName<T>();

        var entry = MessageStoreEntry.CreateOutboxEntry(
            message.Id,
            message.MessageType,
            _serializer.Serialize(message),
            exchangeName,
            routingKey,
            queueName,
            options.Headers != null ? JsonSerializer.Serialize(options.Headers) : null,
            message.CorrelationId);

        await _repository.AddAsync(entry, cancellationToken);

        _logger.LogDebug("Added message {MessageId} to outbox for exchange '{Exchange}' with routing key '{RoutingKey}' and queue '{QueueName}'",
            message.Id, exchangeName, routingKey, queueName);
    }

    public Task PublishAsync<T>(T @event, CancellationToken cancellationToken = default) where T : IEvent
    {
        var exchangeName = _routingResolver.GetExchangeName<T>();
        var routingKey = _routingResolver.GetRoutingKey<T>();
        return PublishAsync(@event, exchangeName, routingKey, cancellationToken);
    }

    public Task SendAsync<T>(T command, CancellationToken cancellationToken = default) where T : ICommand
    {
        var queueName = _routingResolver.GetQueueName<T>();
        return SendAsync(command, queueName, cancellationToken);
    }

    public Task SendAsync<T>(T command, string queueName, CancellationToken cancellationToken = default) where T : ICommand
    {
        return PublishAsync(command, string.Empty, queueName, cancellationToken);
    }
}

