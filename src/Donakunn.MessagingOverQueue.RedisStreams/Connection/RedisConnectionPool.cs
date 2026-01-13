using Donakunn.MessagingOverQueue.RedisStreams.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Donakunn.MessagingOverQueue.RedisStreams.Connection;

/// <summary>
/// Manages Redis connections using StackExchange.Redis ConnectionMultiplexer.
/// </summary>
public sealed class RedisConnectionPool : IRedisConnectionPool
{
    private readonly RedisStreamsOptions _options;
    private readonly ILogger<RedisConnectionPool> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    private IConnectionMultiplexer? _connection;
    private bool _disposed;

    public RedisConnectionPool(
        IOptions<RedisStreamsOptions> options,
        ILogger<RedisConnectionPool> logger)
    {
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public bool IsConnected => _connection?.IsConnected ?? false;

    /// <inheritdoc />
    public IConnectionMultiplexer GetConnection()
    {
        if (_connection == null || !_connection.IsConnected)
        {
            throw new InvalidOperationException(
                "Redis connection is not established. Call EnsureConnectedAsync first.");
        }

        return _connection;
    }

    /// <inheritdoc />
    public IDatabase GetDatabase()
    {
        return GetConnection().GetDatabase(_options.Database);
    }

    /// <inheritdoc />
    public ISubscriber GetSubscriber()
    {
        return GetConnection().GetSubscriber();
    }

    /// <inheritdoc />
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

    private async Task CreateConnectionAsync(CancellationToken cancellationToken)
    {
        var configOptions = BuildConfigurationOptions();

        _logger.LogInformation(
            "Connecting to Redis at {Endpoints}",
            string.Join(", ", configOptions.EndPoints.Select(e => e.ToString())));

        try
        {
            _connection = await ConnectionMultiplexer.ConnectAsync(configOptions);

            // Register event handlers
            _connection.ConnectionFailed += OnConnectionFailed;
            _connection.ConnectionRestored += OnConnectionRestored;
            _connection.ErrorMessage += OnErrorMessage;
            _connection.InternalError += OnInternalError;

            _logger.LogInformation(
                "Successfully connected to Redis. Server version: {Version}",
                _connection.GetServer(_connection.GetEndPoints().First()).Version);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to connect to Redis");
            throw;
        }
    }

    private ConfigurationOptions BuildConfigurationOptions()
    {
        var config = ConfigurationOptions.Parse(_options.ConnectionString);

        // Override with explicit options
        if (!string.IsNullOrEmpty(_options.Password))
        {
            config.Password = _options.Password;
        }

        config.DefaultDatabase = _options.Database;
        config.ConnectTimeout = (int)_options.ConnectTimeout.TotalMilliseconds;
        config.SyncTimeout = (int)_options.SyncTimeout.TotalMilliseconds;
        config.AsyncTimeout = (int)_options.AsyncTimeout.TotalMilliseconds;
        config.ConnectRetry = _options.ConnectRetry;
        config.AbortOnConnectFail = _options.AbortOnConnectFail;

        if (!string.IsNullOrEmpty(_options.ClientName))
        {
            config.ClientName = _options.ClientName;
        }
        else
        {
            config.ClientName = $"MessagingOverQueue-{Environment.MachineName}";
        }

        if (_options.UseSsl)
        {
            config.Ssl = true;
            if (!string.IsNullOrEmpty(_options.SslHost))
            {
                config.SslHost = _options.SslHost;
            }
        }

        return config;
    }

    private void OnConnectionFailed(object? sender, ConnectionFailedEventArgs e)
    {
        _logger.LogWarning(
            "Redis connection failed. Endpoint: {Endpoint}, FailureType: {FailureType}, Exception: {Exception}",
            e.EndPoint, e.FailureType, e.Exception?.Message);
    }

    private void OnConnectionRestored(object? sender, ConnectionFailedEventArgs e)
    {
        _logger.LogInformation(
            "Redis connection restored. Endpoint: {Endpoint}",
            e.EndPoint);
    }

    private void OnErrorMessage(object? sender, RedisErrorEventArgs e)
    {
        _logger.LogWarning("Redis error: {Message}", e.Message);
    }

    private void OnInternalError(object? sender, InternalErrorEventArgs e)
    {
        _logger.LogError(e.Exception, "Redis internal error: {Origin}", e.Origin);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        if (_connection != null)
        {
            // Unsubscribe from events
            _connection.ConnectionFailed -= OnConnectionFailed;
            _connection.ConnectionRestored -= OnConnectionRestored;
            _connection.ErrorMessage -= OnErrorMessage;
            _connection.InternalError -= OnInternalError;

            try
            {
                await _connection.CloseAsync();
                _connection.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error closing Redis connection");
            }
        }

        _connectionLock.Dispose();
        _logger.LogInformation("Redis connection pool disposed");
    }
}
