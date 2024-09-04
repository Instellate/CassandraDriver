using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using CassandraDriver.Results;

namespace CassandraDriver.Frames.Response;

internal class CqlRows : Query
{
    internal required CqlQueryResponseFlags Flags { get; init; }
    internal CqlGlobalTableSpec? GlobalTableSpec { get; init; }
    internal CqlBytes? PagingState { get; init; }

    public override Row this[int index] => this.Rows[index];
    public override int Count => this.Rows.Count;

    public static Query Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        CqlQueryResponseFlags flags =
            (CqlQueryResponseFlags)BinaryPrimitives.ReadInt32BigEndian(bytes);
        bytes = bytes[sizeof(int)..];

        int columnCount = BinaryPrimitives.ReadInt32BigEndian(bytes);
        bytes = bytes[sizeof(int)..];

        CqlBytes? pagingState = null;
        if ((flags & CqlQueryResponseFlags.HasMorePages) != 0)
        {
            pagingState = CqlBytes.Deserialize(ref bytes);
        }

        CqlGlobalTableSpec? spec = null;
        if ((flags & CqlQueryResponseFlags.GlobalTableSpec) != 0)
        {
            spec = CqlGlobalTableSpec.Deserialize(ref bytes);
        }

        if ((flags & CqlQueryResponseFlags.NoMetadata) != 0)
        {
            throw new CassandraException("What? How? You are not suppose to do this!!");
        }

        List<Column> columns = new(columnCount);
        for (int i = 0; i < columnCount; i++)
        {
            columns.Add(Column.Deserialize(
                    ref bytes,
                    (flags & CqlQueryResponseFlags.GlobalTableSpec) == 0
                )
            );
        }

        int rowsCount = BinaryPrimitives.ReadInt32BigEndian(bytes);
        bytes = bytes[sizeof(int)..];

        List<Row> rows = new(rowsCount);
        for (int i = 0; i < rowsCount; i++)
        {
            rows.Add(Row.Deserialize(ref bytes, columns));
        }

        CqlRows cqlRows = new()
        {
            Flags = flags,
            Rows = rows,
            GlobalTableSpec = spec,
            PagingState = pagingState
        };

        return cqlRows;
    }
}
