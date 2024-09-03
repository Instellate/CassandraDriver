namespace CassandraDriver.Results;

public class BindMarker
{
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

    public string Name { get; init; }
    public ColumnValueType Type { get; init; }

    /// <summary>
    /// The partition key index. Is -1 if it isn't a partition key.
    /// </summary>
    public int PartitionKeyIndex { get; init; }

    public string? Keyspace { get; init; }
    public string? Table { get; init; }
}
