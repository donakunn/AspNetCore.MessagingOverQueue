using MessagingOverQueue.src.Configuration.Options;
using MessagingOverQueue.src.Connection;
using MessagingOverQueue.src.Consuming.Handlers;
using MessagingOverQueue.src.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MessagingOverQueue.src.Hosting;

/// <summary>
/// Hosted service that manages RabbitMQ topology and lifecycle.
/// </summary>
public class RabbitMqHostedService(
    IRabbitMqConnectionPool connectionPool,
    IHandlerInvokerRegistry handlerInvokerRegistry,
    IHandlerInvokerFactory handlerInvokerFactory,
    IEnumerable<IHandlerInvokerRegistration> handlerInvokerRegistrations,
    IEnumerable<HandlerScanRegistration> scanRegistrations,
    IHandlerInvokerScanner handlerInvokerScanner,
    IOptions<RabbitMqOptions> options,
    ILogger<RabbitMqHostedService> logger) : IHostedService
{
    private readonly RabbitMqOptions _options = options.Value;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting RabbitMQ hosted service");

        // Initialize handler invokers (reflection-free dispatch setup)
        InitializeHandlerInvokers();

        await connectionPool.EnsureConnectedAsync(cancellationToken);
        await DeclareTopologyAsync(cancellationToken);

        logger.LogInformation("RabbitMQ hosted service started");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Stopping RabbitMQ hosted service");
        await connectionPool.DisposeAsync();
        logger.LogInformation("RabbitMQ hosted service stopped");
    }

    private async Task DeclareTopologyAsync(CancellationToken cancellationToken)
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
                logger.LogDebug("Binding queue '{Queue}' to exchange '{Exchange}' with routing key '{RoutingKey}'",
                    binding.Queue, binding.Exchange, binding.RoutingKey);

                await channel.QueueBindAsync(
                    queue: binding.Queue,
                    exchange: binding.Exchange,
                    routingKey: binding.RoutingKey,
                    arguments: binding.Arguments,
                    cancellationToken: cancellationToken);
            }

            logger.LogInformation("RabbitMQ topology declared successfully");
        }
        finally
        {
            connectionPool.ReturnChannel(channel);
        }
    }

    /// <summary>
    /// Initializes handler invokers from explicit registrations and assembly scans.
    /// This is done once at startup to enable reflection-free handler dispatch.
    /// </summary>
    private void InitializeHandlerInvokers()
    {
        logger.LogDebug("Initializing handler invokers");

        // Register invokers from explicit AddHandler<THandler, TMessage>() calls
        foreach (var registration in handlerInvokerRegistrations)
        {
            registration.Register(handlerInvokerRegistry, handlerInvokerFactory);
        }

        // Register invokers from assembly scans
        foreach (var scanRegistration in scanRegistrations)
        {
            if (scanRegistration.Assemblies.Length > 0)
            {
                handlerInvokerScanner.ScanAndRegister(scanRegistration.Assemblies);
            }
        }

        logger.LogDebug("Handler invokers initialized");
    }
}

