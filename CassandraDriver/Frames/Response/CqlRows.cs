using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using CassandraDriver.Results;

namespace CassandraDriver.Frames.Response;

internal class CqlRows : Query
{
    private readonly CqlQueryResponseFlags _flags;
    private readonly CqlGlobalTableSpec? _globalTableSpec;
    private readonly CqlBytes? _pagingState;
    private readonly IReadOnlyList<Row> _rows;

    // This is here for later when paging is going to be implemented
    private readonly CassandraClient _client;

    public override IReadOnlyList<Row> Rows => this._rows;

    public CqlRows(IReadOnlyList<Row> rows,
        CqlQueryResponseFlags flags,
        CqlGlobalTableSpec? globalTableSpec,
        CqlBytes? pagingState,
        CassandraClient client)
    {
        this._rows = rows;
        this._flags = flags;
        this._globalTableSpec = globalTableSpec;
        this._pagingState = pagingState;
        this._client = client;
    }

    public override Row this[int index] => this.Rows[index];
    public override int Count => this.Rows.Count;

    public static Query Deserialize(ref ReadOnlySpan<byte> bytes,
        CassandraClient clientRef)
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

        return new CqlRows(rows, flags, spec, pagingState, clientRef);
    }
}
