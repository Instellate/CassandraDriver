using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using CassandraDriver.Frames;
using CassandraDriver.Frames.Response;

namespace CassandraDriver.Results;

public class Column
{
    internal Column(
        CqlString name,
        ColumnValue value,
        CqlString? keyspace = null,
        CqlString? table = null
    )
    {
        this.Name = name.Value;
        this.Keyspace = keyspace?.Value;
        this.Table = table?.Value;
        this.Value = value;
    }

    public string Name { get; init; }
    public ColumnValue Value { get; init; }
    public string? Keyspace { get; init; }
    public string? Table { get; init; }

    internal static Column Deserialize(
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
        ColumnValue value = ColumnValue.Deserialize(ref bytes);

        return new Column(
            name,
            value,
            keyspace,
            table
        );
    }

    internal static IReadOnlyList<Column> DeseralizeColumns(ref ReadOnlySpan<byte> bytes,
        IReadOnlyList<Column>? preColumns = null)
    {
        CqlQueryResponseFlags flags =
            (CqlQueryResponseFlags)BinaryPrimitives.ReadInt32BigEndian(bytes);
        bytes = bytes[sizeof(int)..];

        int columnCount = BinaryPrimitives.ReadInt32BigEndian(bytes);
        bytes = bytes[sizeof(int)..];

        if ((flags & CqlQueryResponseFlags.HasMorePages) != 0)
        {
            CqlBytes.Deserialize(ref bytes);
        }

        if ((flags & CqlQueryResponseFlags.GlobalTableSpec) != 0)
        {
            CqlGlobalTableSpec.Deserialize(ref bytes);
        }

        IReadOnlyList<Column> columns;
        if ((flags & CqlQueryResponseFlags.NoMetadata) != 0)
        {
            if (preColumns is not null)
            {
                columns = preColumns;
            }
            else
            {
                throw new CassandraException(
                    "Got no metadata flag without any pre filled columns");
            }
        }
        else
        {
            List<Column> newColumns = new List<Column>(columnCount);
            for (int i = 0; i < columnCount; i++)
            {
                newColumns.Add(Deserialize(
                        ref bytes,
                        (flags & CqlQueryResponseFlags.GlobalTableSpec) == 0
                    )
                );
            }

            columns = newColumns;
        }

        return columns;
    }
}
