using MessagingOverQueue.src.Abstractions.Consuming;
using MessagingOverQueue.src.Configuration.Options;
using MessagingOverQueue.src.Connection;
using MessagingOverQueue.src.Consuming;
using MessagingOverQueue.src.Consuming.Handlers;
using MessagingOverQueue.src.Consuming.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessagingOverQueue.src.Hosting;

/// <summary>
/// Hosted service that manages message consumers.
/// </summary>
public class ConsumerHostedService(
    IServiceProvider serviceProvider,
    IEnumerable<ConsumerRegistration> registrations,
    ILogger<ConsumerHostedService> logger) : IHostedService, IAsyncDisposable
{
    private readonly List<IMessageConsumer> _consumers = new();

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting consumer hosted service");

        foreach (var registration in registrations)
        {
            var consumer = CreateConsumer(registration);
            _consumers.Add(consumer);

            await consumer.StartAsync(cancellationToken);
            logger.LogInformation("Started consumer for queue '{Queue}'", registration.Options.QueueName);
        }

        logger.LogInformation("All consumers started ({Count} total)", _consumers.Count);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Stopping consumer hosted service");

        var stopTasks = _consumers.Select(c => c.StopAsync(cancellationToken));
        await Task.WhenAll(stopTasks);

        logger.LogInformation("All consumers stopped");
    }

    private RabbitMqConsumer CreateConsumer(ConsumerRegistration registration)
    {
        var connectionPool = serviceProvider.GetRequiredService<IRabbitMqConnectionPool>();
        var handlerInvokerRegistry = serviceProvider.GetRequiredService<IHandlerInvokerRegistry>();
        var middlewares = serviceProvider.GetServices<IConsumeMiddleware>();
        var consumerLogger = serviceProvider.GetRequiredService<ILogger<RabbitMqConsumer>>();

        return new RabbitMqConsumer(
            connectionPool,
            serviceProvider,
            handlerInvokerRegistry,
            registration.Options,
            middlewares,
            consumerLogger);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var consumer in _consumers)
        {
            await consumer.DisposeAsync();
        }
        _consumers.Clear();
    }
}

/// <summary>
/// Registration for a consumer.
/// </summary>
public class ConsumerRegistration
{
    public ConsumerOptions Options { get; init; } = new();
    public Type? HandlerType { get; init; }
}

