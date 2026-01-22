using System.Data;
using Donakunn.MessagingOverQueue.Persistence.Entities;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Donakunn.MessagingOverQueue.Persistence.Providers.SqlServer;

/// <summary>
/// SQL Server implementation of <see cref="IMessageStoreProvider"/> using ADO.NET.
/// Provides high-performance, reliable message store operations.
/// </summary>
public sealed class SqlServerMessageStoreProvider : IMessageStoreProvider
{
    private readonly MessageStoreOptions _options;
    private readonly ILogger<SqlServerMessageStoreProvider> _logger;
    private readonly string _tableName;

    public SqlServerMessageStoreProvider(
        IOptions<MessageStoreOptions> options,
        ILogger<SqlServerMessageStoreProvider> logger)
    {
        _options = options.Value;
        _logger = logger;

        _tableName = string.IsNullOrEmpty(_options.Schema)
            ? $"[{_options.TableName}]"
            : $"[{_options.Schema}].[{_options.TableName}]";
    }

    public async Task AddAsync(MessageStoreEntry entry, CancellationToken cancellationToken = default)
    {
        await using var connection = await CreateConnectionAsync(cancellationToken);
        await InsertEntryAsync(connection, null, entry, cancellationToken);
    }

    public async Task AddAsync(MessageStoreEntry entry, ITransactionContext transactionContext, CancellationToken cancellationToken = default)
    {
        if (transactionContext is not SqlServerTransactionContext sqlContext)
        {
            throw new ArgumentException("Transaction context must be a SQL Server transaction context.", nameof(transactionContext));
        }

        await InsertEntryAsync(sqlContext.Connection, sqlContext.Transaction, entry, cancellationToken);
    }

    public async Task<bool> TryAddAsync(MessageStoreEntry entry, CancellationToken cancellationToken = default)
    {
        await using var connection = await CreateConnectionAsync(cancellationToken);
        return await TryInsertEntryAsync(connection, null, entry, cancellationToken);
    }

    private async Task InsertEntryAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        MessageStoreEntry entry,
        CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO {0} (
                Id, Direction, MessageType, Payload, ExchangeName, RoutingKey, QueueName, Headers,
                HandlerType, CreatedAt, ProcessedAt, Status, RetryCount, LastError,
                LockToken, LockExpiresAt, CorrelationId
            ) VALUES (
                @Id, @Direction, @MessageType, @Payload, @ExchangeName, @RoutingKey, @QueueName, @Headers,
                @HandlerType, @CreatedAt, @ProcessedAt, @Status, @RetryCount, @LastError,
                @LockToken, @LockExpiresAt, @CorrelationId
            )
            """;

        await using var command = new SqlCommand(string.Format(sql, _tableName), connection, transaction);
        command.CommandTimeout = _options.CommandTimeoutSeconds;

        AddParameters(command, entry);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<bool> TryInsertEntryAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        MessageStoreEntry entry,
        CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO {0} (
                Id, Direction, MessageType, Payload, ExchangeName, RoutingKey, QueueName, Headers,
                HandlerType, CreatedAt, ProcessedAt, Status, RetryCount, LastError,
                LockToken, LockExpiresAt, CorrelationId
            ) VALUES (
                @Id, @Direction, @MessageType, @Payload, @ExchangeName, @RoutingKey, @QueueName, @Headers,
                @HandlerType, @CreatedAt, @ProcessedAt, @Status, @RetryCount, @LastError,
                @LockToken, @LockExpiresAt, @CorrelationId
            )
            """;

        await using var command = new SqlCommand(string.Format(sql, _tableName), connection, transaction);
        command.CommandTimeout = _options.CommandTimeoutSeconds;

        AddParameters(command, entry);

        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
            return true;
        }
        catch (SqlException ex) when (ex.Number == 2627 || ex.Number == 2601)
        {
            // 2627 = Violation of PRIMARY KEY constraint
            // 2601 = Cannot insert duplicate key row in object (unique index)
            return false;
        }
    }

    public async Task<MessageStoreEntry?> GetByIdAsync(Guid id, MessageDirection direction, CancellationToken cancellationToken = default)
    {
        const string sql = """
            SELECT Id, Direction, MessageType, Payload, ExchangeName, RoutingKey, QueueName, Headers,
                   HandlerType, CreatedAt, ProcessedAt, Status, RetryCount, LastError,
                   LockToken, LockExpiresAt, CorrelationId
            FROM {0}
            WHERE Id = @Id AND Direction = @Direction
            """;

        await using var connection = await CreateConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(string.Format(sql, _tableName), connection);
        command.CommandTimeout = _options.CommandTimeoutSeconds;
        command.Parameters.AddWithValue("@Id", id);
        command.Parameters.AddWithValue("@Direction", (int)direction);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return MapEntry(reader);
        }

        return null;
    }

    public async Task<bool> ExistsInboxEntryAsync(Guid messageId, string handlerType, CancellationToken cancellationToken = default)
    {
        const string sql = """
            SELECT 1 FROM {0}
            WHERE Id = @Id AND Direction = @Direction AND HandlerType = @HandlerType
            """;

        await using var connection = await CreateConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(string.Format(sql, _tableName), connection);
        command.CommandTimeout = _options.CommandTimeoutSeconds;
        command.Parameters.AddWithValue("@Id", messageId);
        command.Parameters.AddWithValue("@Direction", (int)MessageDirection.Inbox);
        command.Parameters.AddWithValue("@HandlerType", handlerType);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result != null;
    }

    public async Task<IReadOnlyList<MessageStoreEntry>> AcquireOutboxLockAsync(
        int batchSize,
        TimeSpan lockDuration,
        CancellationToken cancellationToken = default)
    {
        var lockToken = Guid.NewGuid().ToString("N");
        var now = DateTime.UtcNow;
        var lockExpiresAt = now.Add(lockDuration);

        // Use OUTPUT clause for atomic select-and-update
        const string sql = """
            UPDATE TOP (@BatchSize) {0}
            SET Status = @ProcessingStatus,
                LockToken = @LockToken,
                LockExpiresAt = @LockExpiresAt
            OUTPUT inserted.Id, inserted.Direction, inserted.MessageType, inserted.Payload,
                   inserted.ExchangeName, inserted.RoutingKey, inserted.QueueName, inserted.Headers,
                   inserted.HandlerType, inserted.CreatedAt, inserted.ProcessedAt,
                   inserted.Status, inserted.RetryCount, inserted.LastError,
                   inserted.LockToken, inserted.LockExpiresAt, inserted.CorrelationId
            WHERE Direction = @OutboxDirection
              AND (Status = @PendingStatus OR (Status = @ProcessingStatus AND LockExpiresAt < @Now))
            """;

        await using var connection = await CreateConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(string.Format(sql, _tableName), connection);
        command.CommandTimeout = _options.CommandTimeoutSeconds;
        command.Parameters.AddWithValue("@BatchSize", batchSize);
        command.Parameters.AddWithValue("@ProcessingStatus", (int)MessageStatus.Processing);
        command.Parameters.AddWithValue("@LockToken", lockToken);
        command.Parameters.AddWithValue("@LockExpiresAt", lockExpiresAt);
        command.Parameters.AddWithValue("@OutboxDirection", (int)MessageDirection.Outbox);
        command.Parameters.AddWithValue("@PendingStatus", (int)MessageStatus.Pending);
        command.Parameters.AddWithValue("@Now", now);

        var entries = new List<MessageStoreEntry>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            entries.Add(MapEntry(reader));
        }

        if (entries.Count > 0)
        {
            _logger.LogDebug("Acquired lock on {Count} outbox messages with token {LockToken}", entries.Count, lockToken);
        }

        return entries;
    }

    public async Task<IReadOnlyList<MessageStoreEntry>> AcquireOutboxLockAsync(
        int batchSize,
        TimeSpan lockDuration,
        int[]? assignedPartitions,
        int partitionCount,
        CancellationToken cancellationToken = default)
    {
        // If no partitions specified, delegate to the non-partitioned method
        if (assignedPartitions == null || assignedPartitions.Length == 0)
        {
            return await AcquireOutboxLockAsync(batchSize, lockDuration, cancellationToken);
        }

        var lockToken = Guid.NewGuid().ToString("N");
        var now = DateTime.UtcNow;
        var lockExpiresAt = now.Add(lockDuration);

        // Build partition filter - partition by QueueName using CHECKSUM
        var partitionParams = string.Join(",", assignedPartitions.Select((_, i) => $"@Partition{i}"));
        var sql = $"""
            UPDATE TOP (@BatchSize) {_tableName}
            SET Status = @ProcessingStatus,
                LockToken = @LockToken,
                LockExpiresAt = @LockExpiresAt
            OUTPUT inserted.Id, inserted.Direction, inserted.MessageType, inserted.Payload,
                   inserted.ExchangeName, inserted.RoutingKey, inserted.QueueName, inserted.Headers,
                   inserted.HandlerType, inserted.CreatedAt, inserted.ProcessedAt,
                   inserted.Status, inserted.RetryCount, inserted.LastError,
                   inserted.LockToken, inserted.LockExpiresAt, inserted.CorrelationId
            WHERE Direction = @OutboxDirection
              AND (Status = @PendingStatus OR (Status = @ProcessingStatus AND LockExpiresAt < @Now))
              AND ABS(CHECKSUM(ISNULL(QueueName, ''))) % @PartitionCount IN ({partitionParams})
            """;

        await using var connection = await CreateConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(sql, connection);
        command.CommandTimeout = _options.CommandTimeoutSeconds;
        command.Parameters.AddWithValue("@BatchSize", batchSize);
        command.Parameters.AddWithValue("@ProcessingStatus", (int)MessageStatus.Processing);
        command.Parameters.AddWithValue("@LockToken", lockToken);
        command.Parameters.AddWithValue("@LockExpiresAt", lockExpiresAt);
        command.Parameters.AddWithValue("@OutboxDirection", (int)MessageDirection.Outbox);
        command.Parameters.AddWithValue("@PendingStatus", (int)MessageStatus.Pending);
        command.Parameters.AddWithValue("@Now", now);
        command.Parameters.AddWithValue("@PartitionCount", partitionCount);

        for (int i = 0; i < assignedPartitions.Length; i++)
        {
            command.Parameters.AddWithValue($"@Partition{i}", assignedPartitions[i]);
        }

        var entries = new List<MessageStoreEntry>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            entries.Add(MapEntry(reader));
        }

        if (entries.Count > 0)
        {
            _logger.LogDebug(
                "Acquired lock on {Count} outbox messages with token {LockToken} for partitions [{Partitions}]",
                entries.Count, lockToken, string.Join(",", assignedPartitions));
        }

        return entries;
    }

    public async Task MarkAsPublishedAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        const string sql = """
            UPDATE {0}
            SET Status = @Status,
                ProcessedAt = @ProcessedAt,
                LockToken = NULL,
                LockExpiresAt = NULL
            WHERE Id = @Id AND Direction = @Direction
            """;

        await using var connection = await CreateConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(string.Format(sql, _tableName), connection);
        command.CommandTimeout = _options.CommandTimeoutSeconds;
        command.Parameters.AddWithValue("@Status", (int)MessageStatus.Published);
        command.Parameters.AddWithValue("@ProcessedAt", DateTime.UtcNow);
        command.Parameters.AddWithValue("@Id", messageId);
        command.Parameters.AddWithValue("@Direction", (int)MessageDirection.Outbox);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkAsFailedAsync(Guid messageId, string error, CancellationToken cancellationToken = default)
    {
        var truncatedError = error.Length > 4000 ? error[..4000] : error;

        const string sql = """
            UPDATE {0}
            SET Status = @Status,
                RetryCount = RetryCount + 1,
                LastError = @LastError,
                LockToken = NULL,
                LockExpiresAt = NULL
            WHERE Id = @Id AND Direction = @Direction
            """;

        await using var connection = await CreateConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(string.Format(sql, _tableName), connection);
        command.CommandTimeout = _options.CommandTimeoutSeconds;
        command.Parameters.AddWithValue("@Status", (int)MessageStatus.Failed);
        command.Parameters.AddWithValue("@LastError", truncatedError);
        command.Parameters.AddWithValue("@Id", messageId);
        command.Parameters.AddWithValue("@Direction", (int)MessageDirection.Outbox);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkAsPublishedBatchAsync(IEnumerable<Guid> messageIds, CancellationToken cancellationToken = default)
    {
        var idList = messageIds.ToList();
        if (idList.Count == 0) return;

        // Use table-valued parameter for efficient batch update
        const string sql = """
            UPDATE m
            SET Status = @Status,
                ProcessedAt = @ProcessedAt,
                LockToken = NULL,
                LockExpiresAt = NULL
            FROM {0} m
            INNER JOIN @Ids ids ON m.Id = ids.Id
            WHERE m.Direction = @Direction
            """;

        await using var connection = await CreateConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(string.Format(sql, _tableName), connection);
        command.CommandTimeout = _options.CommandTimeoutSeconds;
        command.Parameters.AddWithValue("@Status", (int)MessageStatus.Published);
        command.Parameters.AddWithValue("@ProcessedAt", DateTime.UtcNow);
        command.Parameters.AddWithValue("@Direction", (int)MessageDirection.Outbox);

        var idsParam = command.Parameters.AddWithValue("@Ids", CreateGuidTable(idList));
        idsParam.SqlDbType = SqlDbType.Structured;
        idsParam.TypeName = "dbo.GuidList";

        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqlException ex) when (ex.Message.Contains("GuidList"))
        {
            // TVP type doesn't exist, fall back to individual updates
            _logger.LogDebug("GuidList type not found, falling back to individual updates");
            await MarkAsPublishedBatchFallbackAsync(idList, cancellationToken);
        }
    }

    private async Task MarkAsPublishedBatchFallbackAsync(List<Guid> messageIds, CancellationToken cancellationToken)
    {
        // Fallback: use IN clause with parameterized IDs (efficient for reasonable batch sizes)
        var paramNames = string.Join(",", messageIds.Select((_, i) => $"@Id{i}"));
        var sql = $"""
            UPDATE {_tableName}
            SET Status = @Status,
                ProcessedAt = @ProcessedAt,
                LockToken = NULL,
                LockExpiresAt = NULL
            WHERE Id IN ({paramNames}) AND Direction = @Direction
            """;

        await using var connection = await CreateConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(sql, connection);
        command.CommandTimeout = _options.CommandTimeoutSeconds;
        command.Parameters.AddWithValue("@Status", (int)MessageStatus.Published);
        command.Parameters.AddWithValue("@ProcessedAt", DateTime.UtcNow);
        command.Parameters.AddWithValue("@Direction", (int)MessageDirection.Outbox);

        for (int i = 0; i < messageIds.Count; i++)
        {
            command.Parameters.AddWithValue($"@Id{i}", messageIds[i]);
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkAsFailedBatchAsync(IEnumerable<(Guid Id, string Error)> failures, CancellationToken cancellationToken = default)
    {
        var failureList = failures.ToList();
        if (failureList.Count == 0) return;

        // For batch failures with different error messages, we need individual updates
        // But we can batch them in a single connection/transaction
        await using var connection = await CreateConnectionAsync(cancellationToken);
        await using var transaction = connection.BeginTransaction();

        try
        {
            const string sql = """
                UPDATE {0}
                SET Status = @Status,
                    RetryCount = RetryCount + 1,
                    LastError = @LastError,
                    LockToken = NULL,
                    LockExpiresAt = NULL
                WHERE Id = @Id AND Direction = @Direction
                """;

            foreach (var (id, error) in failureList)
            {
                var truncatedError = error.Length > 4000 ? error[..4000] : error;

                await using var command = new SqlCommand(string.Format(sql, _tableName), connection, transaction);
                command.CommandTimeout = _options.CommandTimeoutSeconds;
                command.Parameters.AddWithValue("@Status", (int)MessageStatus.Failed);
                command.Parameters.AddWithValue("@LastError", truncatedError);
                command.Parameters.AddWithValue("@Id", id);
                command.Parameters.AddWithValue("@Direction", (int)MessageDirection.Outbox);

                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    private static DataTable CreateGuidTable(IEnumerable<Guid> ids)
    {
        var table = new DataTable();
        table.Columns.Add("Id", typeof(Guid));
        foreach (var id in ids)
        {
            table.Rows.Add(id);
        }
        return table;
    }

    public async Task ReleaseLockAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        const string sql = """
            UPDATE {0}
            SET Status = @Status,
                LockToken = NULL,
                LockExpiresAt = NULL
            WHERE Id = @Id AND Direction = @Direction
            """;

        await using var connection = await CreateConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(string.Format(sql, _tableName), connection);
        command.CommandTimeout = _options.CommandTimeoutSeconds;
        command.Parameters.AddWithValue("@Status", (int)MessageStatus.Pending);
        command.Parameters.AddWithValue("@Id", messageId);
        command.Parameters.AddWithValue("@Direction", (int)MessageDirection.Outbox);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task CleanupAsync(MessageDirection direction, TimeSpan retentionPeriod, CancellationToken cancellationToken = default)
    {
        var cutoffDate = DateTime.UtcNow.Subtract(retentionPeriod);

        string sql;
        if (direction == MessageDirection.Outbox)
        {
            sql = """
                DELETE FROM {0}
                WHERE Direction = @Direction
                  AND Status = @PublishedStatus
                  AND ProcessedAt < @CutoffDate
                """;
        }
        else
        {
            sql = """
                DELETE FROM {0}
                WHERE Direction = @Direction
                  AND ProcessedAt < @CutoffDate
                """;
        }

        await using var connection = await CreateConnectionAsync(cancellationToken);
        await using var command = new SqlCommand(string.Format(sql, _tableName), connection);
        command.CommandTimeout = _options.CommandTimeoutSeconds;
        command.Parameters.AddWithValue("@Direction", (int)direction);
        command.Parameters.AddWithValue("@CutoffDate", cutoffDate);

        if (direction == MessageDirection.Outbox)
        {
            command.Parameters.AddWithValue("@PublishedStatus", (int)MessageStatus.Published);
        }

        var deleted = await command.ExecuteNonQueryAsync(cancellationToken);
        if (deleted > 0)
        {
            _logger.LogDebug("Cleaned up {Count} {Direction} messages older than {CutoffDate}",
                deleted, direction, cutoffDate);
        }
    }

    public async Task EnsureSchemaAsync(CancellationToken cancellationToken = default)
    {
        if (!_options.AutoCreateSchema)
        {
            return;
        }

        var schemaScript = GetCreateSchemaScript();
        var tableScript = GetCreateTableScript();

        await using var connection = await CreateConnectionAsync(cancellationToken);

        // Create schema if specified
        if (!string.IsNullOrEmpty(_options.Schema))
        {
            await using var schemaCommand = new SqlCommand(schemaScript, connection);
            schemaCommand.CommandTimeout = _options.CommandTimeoutSeconds;
            await schemaCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        // Create table
        await using var tableCommand = new SqlCommand(tableScript, connection);
        tableCommand.CommandTimeout = _options.CommandTimeoutSeconds;
        await tableCommand.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogInformation("Ensured message store schema exists: {TableName}", _tableName);
    }

    public async Task<ITransactionContext> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        var connection = await CreateConnectionAsync(cancellationToken);
        var transaction = connection.BeginTransaction();
        return new SqlServerTransactionContext(connection, transaction);
    }

    private async Task<SqlConnection> CreateConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqlConnection(_options.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    private static void AddParameters(SqlCommand command, MessageStoreEntry entry)
    {
        command.Parameters.AddWithValue("@Id", entry.Id);
        command.Parameters.AddWithValue("@Direction", (int)entry.Direction);
        command.Parameters.AddWithValue("@MessageType", entry.MessageType);
        
        // Payload requires explicit SqlDbType since AddWithValue cannot infer VARBINARY from DBNull
        var payloadParam = command.Parameters.Add("@Payload", System.Data.SqlDbType.VarBinary, -1);
        payloadParam.Value = (object?)entry.Payload ?? DBNull.Value;
        
        command.Parameters.AddWithValue("@ExchangeName", (object?)entry.ExchangeName ?? DBNull.Value);
        command.Parameters.AddWithValue("@RoutingKey", (object?)entry.RoutingKey ?? DBNull.Value);
        command.Parameters.AddWithValue("@QueueName", (object?)entry.QueueName ?? DBNull.Value);
        command.Parameters.AddWithValue("@Headers", (object?)entry.Headers ?? DBNull.Value);
        // HandlerType is NOT NULL in the schema, use empty string as default
        command.Parameters.AddWithValue("@HandlerType", entry.HandlerType ?? string.Empty);
        command.Parameters.AddWithValue("@CreatedAt", entry.CreatedAt);
        command.Parameters.AddWithValue("@ProcessedAt", (object?)entry.ProcessedAt ?? DBNull.Value);
        command.Parameters.AddWithValue("@Status", (int)entry.Status);
        command.Parameters.AddWithValue("@RetryCount", entry.RetryCount);
        command.Parameters.AddWithValue("@LastError", (object?)entry.LastError ?? DBNull.Value);
        command.Parameters.AddWithValue("@LockToken", (object?)entry.LockToken ?? DBNull.Value);
        command.Parameters.AddWithValue("@LockExpiresAt", (object?)entry.LockExpiresAt ?? DBNull.Value);
        command.Parameters.AddWithValue("@CorrelationId", (object?)entry.CorrelationId ?? DBNull.Value);
    }

    private static MessageStoreEntry MapEntry(SqlDataReader reader)
    {
        return new MessageStoreEntry
        {
            Id = reader.GetGuid(0),
            Direction = (MessageDirection)reader.GetInt32(1),
            MessageType = reader.GetString(2),
            Payload = reader.IsDBNull(3) ? null : (byte[])reader[3],
            ExchangeName = reader.IsDBNull(4) ? null : reader.GetString(4),
            RoutingKey = reader.IsDBNull(5) ? null : reader.GetString(5),
            QueueName = reader.IsDBNull(6) ? null : reader.GetString(6),
            Headers = reader.IsDBNull(7) ? null : reader.GetString(7),
            HandlerType = reader.IsDBNull(8) ? null : reader.GetString(8),
            CreatedAt = reader.GetDateTime(9),
            ProcessedAt = reader.IsDBNull(10) ? null : reader.GetDateTime(10),
            Status = (MessageStatus)reader.GetInt32(11),
            RetryCount = reader.GetInt32(12),
            LastError = reader.IsDBNull(13) ? null : reader.GetString(13),
            LockToken = reader.IsDBNull(14) ? null : reader.GetString(14),
            LockExpiresAt = reader.IsDBNull(15) ? null : reader.GetDateTime(15),
            CorrelationId = reader.IsDBNull(16) ? null : reader.GetString(16)
        };
    }

    private string GetCreateSchemaScript()
    {
        return $"""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{_options.Schema}')
            BEGIN
                EXEC('CREATE SCHEMA [{_options.Schema}]')
            END
            """;
    }

    private string GetCreateTableScript()
    {
        var fullTableName = string.IsNullOrEmpty(_options.Schema)
            ? _options.TableName
            : $"{_options.Schema}].[{_options.TableName}";

        return $"""
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{_options.TableName}' 
                {(string.IsNullOrEmpty(_options.Schema) ? "" : $"AND TABLE_SCHEMA = '{_options.Schema}'")})
            BEGIN
                CREATE TABLE [{fullTableName}] (
                    [Id] UNIQUEIDENTIFIER NOT NULL,
                    [Direction] INT NOT NULL,
                    [MessageType] NVARCHAR(500) NOT NULL,
                    [Payload] VARBINARY(MAX) NULL,
                    [ExchangeName] NVARCHAR(256) NULL,
                    [RoutingKey] NVARCHAR(256) NULL,
                    [QueueName] NVARCHAR(256) NULL,
                    [Headers] NVARCHAR(MAX) NULL,
                    [HandlerType] NVARCHAR(500) NOT NULL DEFAULT '',
                    [CreatedAt] DATETIME2 NOT NULL,
                    [ProcessedAt] DATETIME2 NULL,
                    [Status] INT NOT NULL,
                    [RetryCount] INT NOT NULL DEFAULT 0,
                    [LastError] NVARCHAR(4000) NULL,
                    [LockToken] NVARCHAR(100) NULL,
                    [LockExpiresAt] DATETIME2 NULL,
                    [CorrelationId] NVARCHAR(100) NULL,

                    CONSTRAINT [PK_{_options.TableName}] PRIMARY KEY CLUSTERED ([Id], [Direction], [HandlerType])
                        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
                );

                -- Index for outbox processing (pending/processing messages)
                CREATE NONCLUSTERED INDEX [IX_{_options.TableName}_Outbox_Status] 
                ON [{fullTableName}] ([Direction], [Status], [CreatedAt])
                WHERE [Direction] = 0;

                -- Index for lock expiration
                CREATE NONCLUSTERED INDEX [IX_{_options.TableName}_LockExpires] 
                ON [{fullTableName}] ([LockExpiresAt])
                WHERE [LockExpiresAt] IS NOT NULL;

                -- Index for correlation ID tracking
                CREATE NONCLUSTERED INDEX [IX_{_options.TableName}_CorrelationId] 
                ON [{fullTableName}] ([CorrelationId])
                WHERE [CorrelationId] IS NOT NULL;

                -- Index for cleanup operations
                CREATE NONCLUSTERED INDEX [IX_{_options.TableName}_Cleanup] 
                ON [{fullTableName}] ([Direction], [Status], [ProcessedAt])
                WHERE [ProcessedAt] IS NOT NULL;
            END
            """;
    }
}

/// <summary>
/// SQL Server transaction context for batch operations.
/// </summary>
internal sealed class SqlServerTransactionContext : ITransactionContext
{
    internal SqlConnection Connection { get; }
    internal SqlTransaction Transaction { get; }

    public SqlServerTransactionContext(SqlConnection connection, SqlTransaction transaction)
    {
        Connection = connection;
        Transaction = transaction;
    }

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        await Transaction.CommitAsync(cancellationToken);
    }

    public async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        await Transaction.RollbackAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await Transaction.DisposeAsync();
        await Connection.DisposeAsync();
    }
}
