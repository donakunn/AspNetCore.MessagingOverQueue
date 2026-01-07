using MessagingOverQueue.src.Abstractions.Messages;
using MessagingOverQueue.src.Abstractions.Publishing;
using MessagingOverQueue.src.Connection;
using MessagingOverQueue.src.Publishing.Middleware;
using MessagingOverQueue.src.Topology;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace MessagingOverQueue.src.Publishing;

/// <summary>
/// RabbitMQ implementation of message publisher.
/// </summary>
public class RabbitMqPublisher(
    IRabbitMqConnectionPool connectionPool,
    IEnumerable<IPublishMiddleware> middlewares,
    IMessageRoutingResolver routingResolver,
    ILogger<RabbitMqPublisher> logger) : IMessagePublisher, IEventPublisher, ICommandSender
{
    public Task PublishAsync<T>(T message, string? exchangeName = null, string? routingKey = null, CancellationToken cancellationToken = default) where T : IMessage
    {
        return PublishAsync(message, new PublishOptions
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

        var context = new PublishContext
        {
            Message = message,
            MessageType = typeof(T),
            ExchangeName = exchangeName,
            RoutingKey = routingKey,
            Persistent = options.Persistent,
            Priority = options.Priority,
            TimeToLive = options.TimeToLive,
            WaitForConfirm = options.WaitForConfirm,
            ConfirmTimeout = options.ConfirmTimeout
        };

        if (options.Headers != null)
        {
            foreach (var header in options.Headers)
            {
                context.Headers[header.Key] = header.Value;
            }
        }

        var pipeline = new PublishPipeline(middlewares, PublishToRabbitMqAsync);
        await pipeline.ExecuteAsync(context, cancellationToken);
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
        // Commands are sent directly to queue using default exchange
        return PublishAsync(command, string.Empty, queueName, cancellationToken);
    }

    public async Task PublishToRabbitMqAsync(PublishContext context, CancellationToken cancellationToken)
    {
        var channel = await connectionPool.GetChannelAsync(cancellationToken);
        try
        {
            var properties = new BasicProperties
            {
                Persistent = context.Persistent,
                ContentType = context.ContentType,
                MessageId = context.Message.Id.ToString(),
                Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
                Headers = context.Headers.ToDictionary(x => x.Key, x => x.Value)
            };

            if (context.Message.CorrelationId != null)
                properties.CorrelationId = context.Message.CorrelationId;

            if (context.Priority.HasValue)
                properties.Priority = context.Priority.Value;

            if (context.TimeToLive.HasValue)
                properties.Expiration = context.TimeToLive.Value.ToString();

            await channel.BasicPublishAsync(
                exchange: context.ExchangeName ?? string.Empty,
                routingKey: context.RoutingKey ?? string.Empty,
                mandatory: false,
                basicProperties: properties,
                body: context.Body ?? Array.Empty<byte>(),
                cancellationToken: cancellationToken);

            logger.LogDebug(
                "Message {MessageId} published to exchange '{Exchange}' with routing key '{RoutingKey}'",
                context.Message.Id, context.ExchangeName, context.RoutingKey);
        }
        finally
        {
            connectionPool.ReturnChannel(channel);
        }
    }
}

