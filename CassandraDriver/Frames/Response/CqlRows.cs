using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using CassandraDriver.Results;

namespace CassandraDriver.Frames.Response;

internal class CqlRows : Query
{
    private readonly CqlGlobalTableSpec? _globalTableSpec;
    private readonly List<Row> _rows;
    private readonly IReadOnlyList<Column> _columns;
    private readonly CassandraClient _client;
    private readonly BaseStatement _statement;

    // This is here for later when paging is going to be implemented
    private readonly CqlQueryResponseFlags _flags;
    private readonly CqlBytes? _pagingState;

    public override IReadOnlyList<Row> LocalRows => this._rows;
    public override IReadOnlyList<Column> Columns => this._columns;

    public override byte[]? PagingState => this._pagingState?.Bytes;

    private CqlRows(List<Row> rows,
        CqlQueryResponseFlags flags,
        CqlGlobalTableSpec? globalTableSpec,
        CqlBytes? pagingState,
        IReadOnlyList<Column> columns,
        CassandraClient client,
        BaseStatement statement)
    {
        this._rows = rows;
        this._flags = flags;
        this._globalTableSpec = globalTableSpec;
        this._pagingState = pagingState;
        this._columns = columns;
        this._client = client;
        this._statement = statement;
        this._statement.PagingState = pagingState?.Bytes;
    }

    public override Row this[int index] => this.LocalRows[index];
    public override int Count => this.LocalRows.Count;

    public override async IAsyncEnumerator<Row> GetAsyncEnumerator(
        CancellationToken cancellationToken = default)
    {
        foreach (Row row in _rows)
        {
            yield return row;
        }

        CqlQueryResponseFlags flags = this._flags;
        while ((flags & CqlQueryResponseFlags.HasMorePages) != 0)
        {
            Query query
                = await this._client.QueryAsync(this._statement, cancellationToken);

            this._rows.AddRange(query.LocalRows);
            foreach (Row row in query.LocalRows)
            {
                yield return row;
            }

            this._statement.PagingState = query.PagingState;
            flags = ((CqlRows)query)._flags;
        }
    }

    public static Query Deserialize(ref ReadOnlySpan<byte> bytes,
        CassandraClient client,
        BaseStatement statement)
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
            columns = statement.Columns ??
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

        return new CqlRows(rows, flags, spec, pagingState, columns, client, statement);
    }
}
