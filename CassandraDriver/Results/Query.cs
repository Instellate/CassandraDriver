using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using CassandraDriver.Frames;
using CassandraDriver.Frames.Response;

namespace CassandraDriver.Results;

/// <summary>
/// The basic class for results, includes queries, prepare, and execute.
/// Do not inherit and implement it yourself.
/// </summary>
public abstract class Query
{
    private List<String>? _warnings;

    public QueryKind Kind { get; internal init; }
    public IReadOnlyList<Row> Rows { get; internal init; } = null!;
    public string SetKeyspace { get; internal init; } = null!;
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

    internal static Query Deserialize(ReadOnlySpan<byte> bytes,
        CqlStringList? warnings,
        CassandraClient clientRef)
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
                query = CqlRows.Deserialize(ref bytes, clientRef);
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
}
