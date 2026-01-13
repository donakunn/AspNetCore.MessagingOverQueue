using Donakunn.MessagingOverQueue.Abstractions.Messages;
using Donakunn.MessagingOverQueue.Abstractions.Publishing;
using Donakunn.MessagingOverQueue.Publishing.Middleware;
using Donakunn.MessagingOverQueue.Topology;

namespace Donakunn.MessagingOverQueue.RedisStreams;

/// <summary>
/// Redis Streams implementation of the message publisher interfaces.
/// Wraps RedisStreamsPublisher with middleware pipeline support.
/// </summary>
internal sealed class RedisStreamsMessagePublisher : IMessagePublisher, IEventPublisher, ICommandSender
{
    private readonly RedisStreamsPublisher _publisher;
    private readonly IEnumerable<IPublishMiddleware> _middlewares;
    private readonly IMessageRoutingResolver _routingResolver;

    public RedisStreamsMessagePublisher(
        RedisStreamsPublisher publisher,
        IEnumerable<IPublishMiddleware> middlewares,
        IMessageRoutingResolver routingResolver)
    {
        _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        _middlewares = middlewares ?? throw new ArgumentNullException(nameof(middlewares));
        _routingResolver = routingResolver ?? throw new ArgumentNullException(nameof(routingResolver));
    }

    /// <inheritdoc />
    public Task PublishAsync<T>(T message, string? exchangeName = null, string? routingKey = null, CancellationToken cancellationToken = default) where T : IMessage
    {
        return PublishAsync(message, new PublishOptions
        {
            ExchangeName = exchangeName,
            RoutingKey = routingKey
        }, cancellationToken);
    }

    /// <inheritdoc />
    public async Task PublishAsync<T>(T message, PublishOptions options, CancellationToken cancellationToken = default) where T : IMessage
    {
        // Use routing resolver for defaults if not explicitly specified
        var exchangeName = options.ExchangeName ?? _routingResolver.GetExchangeName<T>();
        var routingKey = options.RoutingKey ?? _routingResolver.GetRoutingKey<T>();

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

        var pipeline = new PublishPipeline(_middlewares, _publisher.PublishAsync);
        await pipeline.ExecuteAsync(context, cancellationToken);
    }

    /// <inheritdoc />
    public Task PublishAsync<T>(T @event, CancellationToken cancellationToken = default) where T : IEvent
    {
        var exchangeName = _routingResolver.GetExchangeName<T>();
        var routingKey = _routingResolver.GetRoutingKey<T>();
        return PublishAsync(@event, exchangeName, routingKey, cancellationToken);
    }

    /// <inheritdoc />
    public Task SendAsync<T>(T command, CancellationToken cancellationToken = default) where T : ICommand
    {
        var queueName = _routingResolver.GetQueueName<T>();
        return SendAsync(command, queueName, cancellationToken);
    }

    /// <inheritdoc />
    public Task SendAsync<T>(T command, string queueName, CancellationToken cancellationToken = default) where T : ICommand
    {
        // Commands are sent directly to a specific stream (using queue name as routing key)
        return PublishAsync(command, string.Empty, queueName, cancellationToken);
    }
}
