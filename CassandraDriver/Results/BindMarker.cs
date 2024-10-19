namespace CassandraDriver.Results;

/// <summary>
/// Markers used for binding elements
/// </summary>
public class BindMarker
{
    /// <summary>
    /// The primary constructor for BindMarker
    /// </summary>
    /// <param name="name">The name of the bind marker</param>
    /// <param name="type">The value type for the bind marker</param>
    /// <param name="partitionKeyIndex">The partion key index</param>
    /// <param name="keyspace">The keyspace the bind marker is related to</param>
    /// <param name="table">The table the bind marker is related to</param>
    public BindMarker(
        string name,
        ColumnValueType type,
        int partitionKeyIndex,
        string? keyspace = null,
        string? table = null
    )
    {
        this.Name = name;
        this.Type = type;
        this.PartitionKeyIndex = partitionKeyIndex;
        this.Keyspace = keyspace;
        this.Table = table;
    }

    /// <summary>
    /// The name of the bind marker
    /// </summary>
    public string Name { get; init; }

    /// <summary>
    /// The value type for the bind marker
    /// </summary>
    public ColumnValueType Type { get; init; }

    /// <summary>
    /// The partition key index. Is -1 if it isn't a partition key.
    /// </summary>
    public int PartitionKeyIndex { get; init; }

    /// <summary>
    /// The keyspace the bind marker is related to
    /// </summary>
    public string? Keyspace { get; init; }

    /// <summary>
    /// The table the bind marker is related to
    /// </summary>
    public string? Table { get; init; }
}
