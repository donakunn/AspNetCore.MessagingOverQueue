using Donakunn.MessagingOverQueue.Configuration.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;

namespace Donakunn.MessagingOverQueue.Connection;

/// <summary>
/// Manages a pool of RabbitMQ connections and channels for efficient resource usage.
/// </summary>
public class RabbitMqConnectionPool : IRabbitMqConnectionPool
{
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqConnectionPool> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly ConcurrentQueue<IChannel> _channelPool = new();
    private readonly SemaphoreSlim _channelSemaphore;

    private IConnection? _connection;
    private bool _disposed;

    public RabbitMqConnectionPool(
        IOptions<RabbitMqOptions> options,
        ILogger<RabbitMqConnectionPool> logger)
    {
        _options = options.Value;
        _logger = logger;
        _channelSemaphore = new SemaphoreSlim(_options.ChannelPoolSize, _options.ChannelPoolSize);
    }

    public bool IsConnected => _connection?.IsOpen ?? false;

    public async Task EnsureConnectedAsync(CancellationToken cancellationToken = default)
    {
        if (IsConnected)
            return;

        await _connectionLock.WaitAsync(cancellationToken);
        try
        {
            if (IsConnected)
                return;

            await CreateConnectionAsync(cancellationToken);
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    public async Task<IChannel> GetChannelAsync(CancellationToken cancellationToken = default)
    {
        await EnsureConnectedAsync(cancellationToken);
        await _channelSemaphore.WaitAsync(cancellationToken);

        if (_channelPool.TryDequeue(out var channel) && channel.IsOpen)
        {
            return channel;
        }

        try
        {
            return await CreateChannelAsync(cancellationToken);
        }
        catch
        {
            _channelSemaphore.Release();
            throw;
        }
    }

    public void ReturnChannel(IChannel channel)
    {
        if (channel.IsOpen)
        {
            _channelPool.Enqueue(channel);
        }
        _channelSemaphore.Release();
    }

    public async Task<IChannel> CreateDedicatedChannelAsync(CancellationToken cancellationToken = default)
    {
        await EnsureConnectedAsync(cancellationToken);
        return await CreateChannelAsync(cancellationToken);
    }

    private async Task CreateConnectionAsync(CancellationToken cancellationToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _options.HostName,
            Port = _options.Port,
            UserName = _options.UserName,
            Password = _options.Password,
            VirtualHost = _options.VirtualHost,
            AutomaticRecoveryEnabled = _options.AutomaticRecoveryEnabled,
            NetworkRecoveryInterval = _options.NetworkRecoveryInterval,
            RequestedHeartbeat = _options.RequestedHeartbeat,
            ClientProvidedName = _options.ClientProvidedName ?? $"AsyncMessaging-{Environment.MachineName}"
        };

        if (_options.UseSsl)
        {
            factory.Ssl = new SslOption
            {
                Enabled = true,
                ServerName = _options.SslServerName ?? _options.HostName
            };
        }

        _logger.LogInformation("Connecting to RabbitMQ at {Host}:{Port}/{VirtualHost}",
            _options.HostName, _options.Port, _options.VirtualHost);

        _connection = await factory.CreateConnectionAsync(cancellationToken);

        _connection.ConnectionShutdownAsync += OnConnectionShutdownAsync;
        _connection.ConnectionBlockedAsync += OnConnectionBlockedAsync;
        _connection.ConnectionUnblockedAsync += OnConnectionUnblockedAsync;

        _logger.LogInformation("Successfully connected to RabbitMQ");
    }

    private async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken)
    {
        if (_connection == null || !_connection.IsOpen)
        {
            throw new InvalidOperationException("Connection is not open");
        }

        var channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);

        _logger.LogDebug("Created new channel");
        return channel;
    }

    private Task OnConnectionShutdownAsync(object sender, ShutdownEventArgs args)
    {
        _logger.LogWarning("RabbitMQ connection shutdown: {Reason}", args.ReplyText);
        return Task.CompletedTask;
    }

    private Task OnConnectionBlockedAsync(object sender, ConnectionBlockedEventArgs args)
    {
        _logger.LogWarning("RabbitMQ connection blocked: {Reason}", args.Reason);
        return Task.CompletedTask;
    }

    private Task OnConnectionUnblockedAsync(object sender, AsyncEventArgs args)
    {
        _logger.LogInformation("RabbitMQ connection unblocked");
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        while (_channelPool.TryDequeue(out var channel))
        {
            try
            {
                await channel.CloseAsync();
                channel.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error closing pooled channel");
            }
        }

        if (_connection != null)
        {
            try
            {
                await _connection.CloseAsync();
                _connection.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error closing connection");
            }
        }

        _connectionLock.Dispose();
        _channelSemaphore.Dispose();

        _logger.LogInformation("RabbitMQ connection pool disposed");
    }
}

