using System;
using System.Buffers.Binary;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames.Response;

internal class CqlColumn
{
    public CqlColumn(
        CqlString name,
        CqlColumnValueType type,
        CqlString? keyspace = null,
        CqlString? table = null
    )
    {
        this.Name = name;
        this.Type = type;
        this.Keyspace = keyspace;
        this.Table = table;
    }

    public CqlString Name { get; init; }
    public CqlColumnValueType Type { get; init; }
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
        CqlColumnValueType type =
            (CqlColumnValueType)BinaryPrimitives.ReadInt16BigEndian(bytes);
        bytes = bytes[sizeof(short)..];
        return new CqlColumn(
            name,
            type,
            keyspace,
            table
        );
    }
}
