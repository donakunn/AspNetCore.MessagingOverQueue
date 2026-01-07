using MessagingOverQueue.src.Configuration.Options;
using MessagingOverQueue.src.Connection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MessagingOverQueue.src.Hosting;

/// <summary>
/// Hosted service that manages RabbitMQ connection lifecycle.
/// Single responsibility: ensure connection is established and cleaned up properly.
/// Topology declaration is handled by TopologyInitializationHostedService.
/// </summary>
public sealed class RabbitMqHostedService(
    IRabbitMqConnectionPool connectionPool,
    IOptions<RabbitMqOptions> options,
    ILogger<RabbitMqHostedService> logger) : IHostedService
{
    private readonly RabbitMqOptions _options = options.Value;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting RabbitMQ hosted service");

        await connectionPool.EnsureConnectedAsync(cancellationToken);

        // Declare legacy topology from options (for backwards compatibility)
        if (HasLegacyTopology())
        {
            await DeclareLegacyTopologyAsync(cancellationToken);
        }

        logger.LogInformation("RabbitMQ hosted service started");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Stopping RabbitMQ hosted service");
        await connectionPool.DisposeAsync();
        logger.LogInformation("RabbitMQ hosted service stopped");
    }

    private bool HasLegacyTopology()
    {
        return _options.Exchanges.Count > 0 ||
               _options.Queues.Count > 0 ||
               _options.Bindings.Count > 0;
    }

    /// <summary>
    /// Declares topology from RabbitMqOptions for backwards compatibility.
    /// New code should use AddTopology() instead.
    /// </summary>
    private async Task DeclareLegacyTopologyAsync(CancellationToken cancellationToken)
    {
        var channel = await connectionPool.GetChannelAsync(cancellationToken);
        try
        {
            // Declare exchanges
            foreach (var exchange in _options.Exchanges)
            {
                logger.LogDebug("Declaring exchange '{Exchange}' of type '{Type}'", exchange.Name, exchange.Type);

                await channel.ExchangeDeclareAsync(
                    exchange: exchange.Name,
                    type: exchange.Type,
                    durable: exchange.Durable,
                    autoDelete: exchange.AutoDelete,
                    arguments: exchange.Arguments,
                    cancellationToken: cancellationToken);
            }

            // Declare queues
            foreach (var queue in _options.Queues)
            {
                logger.LogDebug("Declaring queue '{Queue}'", queue.Name);

                var arguments = BuildQueueArguments(queue);

                await channel.QueueDeclareAsync(
                    queue: queue.Name,
                    durable: queue.Durable,
                    exclusive: queue.Exclusive,
                    autoDelete: queue.AutoDelete,
                    arguments: arguments,
                    cancellationToken: cancellationToken);
            }

            // Declare bindings
            foreach (var binding in _options.Bindings)
            {
                logger.LogDebug("Binding queue '{Queue}' to exchange '{Exchange}' with routing key '{RoutingKey}'",
                    binding.Queue, binding.Exchange, binding.RoutingKey);

                await channel.QueueBindAsync(
                    queue: binding.Queue,
                    exchange: binding.Exchange,
                    routingKey: binding.RoutingKey,
                    arguments: binding.Arguments,
                    cancellationToken: cancellationToken);
            }

            logger.LogInformation("Legacy RabbitMQ topology declared successfully");
        }
        finally
        {
            connectionPool.ReturnChannel(channel);
        }
    }

    private static Dictionary<string, object?> BuildQueueArguments(QueueOptions queue)
    {
        var arguments = new Dictionary<string, object?>();

        if (queue.DeadLetterExchange != null)
            arguments["x-dead-letter-exchange"] = queue.DeadLetterExchange;

        if (queue.DeadLetterRoutingKey != null)
            arguments["x-dead-letter-routing-key"] = queue.DeadLetterRoutingKey;

        if (queue.MessageTtl.HasValue)
            arguments["x-message-ttl"] = queue.MessageTtl.Value;

        if (queue.MaxLength.HasValue)
            arguments["x-max-length"] = queue.MaxLength.Value;

        if (queue.MaxLengthBytes.HasValue)
            arguments["x-max-length-bytes"] = queue.MaxLengthBytes.Value;

        if (queue.OverflowBehavior != null)
            arguments["x-overflow"] = queue.OverflowBehavior;

        if (queue.Arguments != null)
        {
            foreach (var arg in queue.Arguments)
            {
                arguments[arg.Key] = arg.Value;
            }
        }

        return arguments;
    }
}

