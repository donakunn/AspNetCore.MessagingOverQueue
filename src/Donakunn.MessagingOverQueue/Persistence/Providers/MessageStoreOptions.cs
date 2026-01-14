namespace Donakunn.MessagingOverQueue.Persistence.Providers;

/// <summary>
/// Configuration options for message store providers.
/// </summary>
public class MessageStoreOptions
{
    /// <summary>
    /// The connection string for the database.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// The schema name for the message store table (if supported by the provider).
    /// </summary>
    public string? Schema { get; set; }

    /// <summary>
    /// The table name for the message store.
    /// </summary>
    public string TableName { get; set; } = "MessageStore";

    /// <summary>
    /// Whether to automatically create the schema/table on startup.
    /// </summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>
    /// Command timeout in seconds for database operations.
    /// </summary>
    public int CommandTimeoutSeconds { get; set; } = 30;
}
