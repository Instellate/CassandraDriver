using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using CassandraDriver.Frames;
using CassandraDriver.Frames.Response;

namespace CassandraDriver.Results;

/// <summary>
/// The basic class for results, includes queries, prepare, and execute.
/// Do not inherit and implement it yourself.
/// </summary>
public abstract class Query : IAsyncEnumerable<Row>
{
    private List<string>? _warnings;

    /// <summary>
    /// The query kind
    /// </summary>
    public QueryKind Kind { get; internal init; }

    /// <summary>
    /// The rows for the query result
    /// </summary>
    public abstract IReadOnlyList<Row> LocalRows { get; }

    /// <summary>
    /// The columns returned by a row query
    /// </summary>
    public abstract IReadOnlyList<Column> Columns { get; }

    /// <summary>
    /// THe set keyspace for the query
    /// </summary>
    public string SetKeyspace { get; internal init; } = null!;

    /// <summary>
    /// Warnings in the query
    /// </summary>
    public IReadOnlyList<string>? Warnings => this._warnings;

    /// <summary>
    /// Index rows
    /// </summary>
    /// <param name="index">The position of where to index.</param>
    public abstract Row this[int index] { get; }

    /// <summary>
    /// Gets the row count
    /// </summary>
    public abstract int Count { get; }

    /// <summary>
    /// The paging state provided if there is anyh
    /// </summary>
    public abstract byte[]? PagingState { get; }

    internal static Query Deserialize(ReadOnlySpan<byte> bytes,
        CqlStringList? warnings,
        CassandraClient client,
        Statement statement)
    {
        QueryKind kind = (QueryKind)BinaryPrimitives.ReadInt32BigEndian(bytes);
        bytes = bytes[sizeof(QueryKind)..];

        Query query;
        switch (kind)
        {
            case QueryKind.Void:
                query = CqlVoid.Instance;
                break;
            case QueryKind.Rows:
                query = CqlRows.Deserialize(ref bytes, client, statement);
                break;
            case QueryKind.SetKeyspace:
                query = CqlSetKeyspace.Deserialize(ref bytes);
                break;
            case QueryKind.Prepared:
                goto default;
            case QueryKind.SchemeChange:
                goto default;
            default:
                throw new ArgumentOutOfRangeException();
        }

        if (warnings?.Strings.Count > 0)
        {
            query._warnings = new List<string>(warnings.Strings.Count);
            foreach (CqlString str in warnings.Strings)
            {
                query._warnings.Add(str.Value);
            }
        }

        return query;
    }

    /// <summary>
    /// Get pages for each row async. Fetches a new page transparently.
    /// </summary>
    /// <remarks>If you are in a sync context and you are sure that there will be no more page fetching you can convert this into a sync enumerable through the extension method</remarks>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public abstract IAsyncEnumerator<Row> GetAsyncEnumerator(
        CancellationToken cancellationToken = default);
}
