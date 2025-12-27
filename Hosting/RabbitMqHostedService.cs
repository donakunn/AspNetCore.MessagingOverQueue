using MessagingOverQueue.Configuration.Options;
using MessagingOverQueue.Connection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace MessagingOverQueue.Hosting;

/// <summary>
/// Hosted service that manages RabbitMQ topology and lifecycle.
/// </summary>
public class RabbitMqHostedService : IHostedService
{
    private readonly IRabbitMqConnectionPool _connectionPool;
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqHostedService> _logger;

    public RabbitMqHostedService(
        IRabbitMqConnectionPool connectionPool,
        IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqHostedService> logger)
    {
        _connectionPool = connectionPool;
        _options = options.Value;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting RabbitMQ hosted service");

        await _connectionPool.EnsureConnectedAsync(cancellationToken);
        await DeclareTopologyAsync(cancellationToken);

        _logger.LogInformation("RabbitMQ hosted service started");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping RabbitMQ hosted service");
        await _connectionPool.DisposeAsync();
        _logger.LogInformation("RabbitMQ hosted service stopped");
    }

    private async Task DeclareTopologyAsync(CancellationToken cancellationToken)
    {
        var channel = await _connectionPool.GetChannelAsync(cancellationToken);
        try
        {
            // Declare exchanges
            foreach (var exchange in _options.Exchanges)
            {
                _logger.LogDebug("Declaring exchange '{Exchange}' of type '{Type}'", exchange.Name, exchange.Type);
                
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
                _logger.LogDebug("Declaring queue '{Queue}'", queue.Name);

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
                _logger.LogDebug("Binding queue '{Queue}' to exchange '{Exchange}' with routing key '{RoutingKey}'",
                    binding.Queue, binding.Exchange, binding.RoutingKey);

                await channel.QueueBindAsync(
                    queue: binding.Queue,
                    exchange: binding.Exchange,
                    routingKey: binding.RoutingKey,
                    arguments: binding.Arguments,
                    cancellationToken: cancellationToken);
            }

            _logger.LogInformation("RabbitMQ topology declared successfully");
        }
        finally
        {
            _connectionPool.ReturnChannel(channel);
        }
    }
}

