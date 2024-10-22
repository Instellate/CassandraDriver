using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using CassandraDriver.Results;

namespace CassandraDriver.Frames.Response;

internal class CqlRows : Query
{
    private readonly CqlGlobalTableSpec? _globalTableSpec;
    private readonly IReadOnlyList<Row> _rows;
    private readonly IReadOnlyList<Column> _columns;

    // This is here for later when paging is going to be implemented
    internal readonly CqlQueryResponseFlags Flags;
    internal readonly CqlBytes? PagingState;

    public override IReadOnlyList<Row> Rows => this._rows;
    public override IReadOnlyList<Column> Columns => this._columns;


    public CqlRows(IReadOnlyList<Row> rows,
        CqlQueryResponseFlags flags,
        CqlGlobalTableSpec? globalTableSpec,
        CqlBytes? pagingState,
        IReadOnlyList<Column> columns)
    {
        this._rows = rows;
        this.Flags = flags;
        this._globalTableSpec = globalTableSpec;
        this.PagingState = pagingState;
        this._columns = columns;
    }

    public override Row this[int index] => this.Rows[index];
    public override int Count => this.Rows.Count;

    public static Query Deserialize(ref ReadOnlySpan<byte> bytes,
        IReadOnlyList<Column>? cachedColumns)
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

        IReadOnlyList<Column> columns;
        if ((flags & CqlQueryResponseFlags.NoMetadata) != 0)
        {
            columns = cachedColumns ??
                      throw new CassandraException(
                          "Got no cached columns and no metadata");
        }
        else
        {
            List<Column> newColumns = new(columnCount);
            for (int i = 0; i < columnCount; i++)
            {
                newColumns.Add(Column.Deserialize(
                        ref bytes,
                        (flags & CqlQueryResponseFlags.GlobalTableSpec) == 0
                    )
                );
            }

            columns = newColumns;
        }


        int rowsCount = BinaryPrimitives.ReadInt32BigEndian(bytes);
        bytes = bytes[sizeof(int)..];

        List<Row> rows = new(rowsCount);
        for (int i = 0; i < rowsCount; i++)
        {
            rows.Add(Row.Deserialize(ref bytes, columns));
        }

        return new CqlRows(rows, flags, spec, pagingState, columns);
    }
}
