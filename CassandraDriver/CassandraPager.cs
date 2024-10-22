using System.Collections.Generic;
using System.Linq;
using System.Threading;
using CassandraDriver.Frames.Response;
using CassandraDriver.Results;

namespace CassandraDriver;

/// <summary>
/// Class that takes care of async paging if there's a lot of rows.
/// </summary>
public class CassandraPager : IAsyncEnumerable<Row>
{
    private readonly List<Row> _cachedRows;
    private readonly IReadOnlyList<Column> _columns;
    private readonly object[] _params;
    private readonly object _query;
    private readonly CassandraClient _client;
    private byte[]? _pagingState;
    private bool _hasMorePages;
    private int _pageSize;

    internal CassandraPager(IReadOnlyList<Row> cachedRows,
        IReadOnlyList<Column> columns,
        object[] @params,
        object query,
        CassandraClient client,
        bool hasMorePages,
        byte[]? pagingState,
        int pageSize)
    {
        this._cachedRows = cachedRows.ToList();
        this._columns = columns;
        this._params = @params;
        this._query = query;
        this._client = client;
        this._hasMorePages = hasMorePages;
        this._pagingState = pagingState;
        this._pageSize = pageSize;
    }

    public async IAsyncEnumerator<Row> GetAsyncEnumerator(
        CancellationToken cancellationToken = default)
    {
        foreach (Row row in this._cachedRows)
        {
            yield return row;
        }

        while (this._hasMorePages)
        {
            Query query;
            if (this._query is string str)
            {
                query = await this._client.QueryWithPagingStateAsync(str,
                    this._params,
                    this._pagingState!,
                    this._columns,
                    this._pageSize);
            }
            else if (this._query is byte[] bytes)
            {
                query = await this._client.ExecuteWithPagingAsync(bytes,
                    this._params,
                    this._pagingState!,
                    this._columns,
                    this._pageSize);
            }
            else
            {
                throw new CassandraException("This was unexpected");
            }

            foreach (Row row in query)
            {
                yield return row;
            }

            if ((((CqlRows)query).Flags & CqlQueryResponseFlags.HasMorePages) != 0)
            {
                this._hasMorePages = true;
                this._pagingState = ((CqlRows)query).PagingState!.Bytes!.ToArray();
            }
            else
            {
                this._hasMorePages = false;
            }
        }
    }
}
