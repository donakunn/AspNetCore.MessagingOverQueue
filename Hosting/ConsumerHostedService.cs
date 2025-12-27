using MessagingOverQueue.Abstractions.Consuming;
using MessagingOverQueue.Configuration.Options;
using MessagingOverQueue.Connection;
using MessagingOverQueue.Consuming;
using MessagingOverQueue.Consuming.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessagingOverQueue.Hosting;

/// <summary>
/// Hosted service that manages message consumers.
/// </summary>
public class ConsumerHostedService : IHostedService, IAsyncDisposable
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IEnumerable<ConsumerRegistration> _registrations;
    private readonly ILogger<ConsumerHostedService> _logger;
    private readonly List<IMessageConsumer> _consumers = new();

    public ConsumerHostedService(
        IServiceProvider serviceProvider,
        IEnumerable<ConsumerRegistration> registrations,
        ILogger<ConsumerHostedService> logger)
    {
        _serviceProvider = serviceProvider;
        _registrations = registrations;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting consumer hosted service");

        foreach (var registration in _registrations)
        {
            var consumer = CreateConsumer(registration);
            _consumers.Add(consumer);
            
            await consumer.StartAsync(cancellationToken);
            _logger.LogInformation("Started consumer for queue '{Queue}'", registration.Options.QueueName);
        }

        _logger.LogInformation("All consumers started ({Count} total)", _consumers.Count);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping consumer hosted service");

        var stopTasks = _consumers.Select(c => c.StopAsync(cancellationToken));
        await Task.WhenAll(stopTasks);

        _logger.LogInformation("All consumers stopped");
    }

    private IMessageConsumer CreateConsumer(ConsumerRegistration registration)
    {
        var connectionPool = _serviceProvider.GetRequiredService<IRabbitMqConnectionPool>();
        var middlewares = _serviceProvider.GetServices<IConsumeMiddleware>();
        var logger = _serviceProvider.GetRequiredService<ILogger<RabbitMqConsumer>>();

        return new RabbitMqConsumer(
            connectionPool,
            _serviceProvider,
            registration.Options,
            middlewares,
            logger);
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

