using System;
using System.Buffers.Binary;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames.Response;

internal class CqlColumn
{
    public CqlColumn(
        CqlString name,
        CqlColumnValue value,
        CqlString? keyspace = null,
        CqlString? table = null
    )
    {
        this.Name = name;
        this.Keyspace = keyspace;
        this.Table = table;
        this.Value = value;
    }

    public CqlString Name { get; init; }
    public CqlColumnValue Value { get; init; }
    public CqlString? Keyspace { get; init; }
    public CqlString? Table { get; init; }

    public static CqlColumn Deserialize(
        ref ReadOnlySpan<byte> bytes,
        bool isGlobalSpecPresent = true
    )
    {
        CqlString? keyspace = null;
        CqlString? table = null;
        if (isGlobalSpecPresent)
        {
            keyspace = CqlString.Deserialize(ref bytes);
            table = CqlString.Deserialize(ref bytes);
        }

        CqlString name = CqlString.Deserialize(ref bytes);
        CqlColumnValue value = CqlColumnValue.Deserialize(ref bytes);

        return new CqlColumn(
            name,
            value,
            keyspace,
            table
        );
    }
}
