using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using CassandraDriver.Frames;
using CassandraDriver.Frames.Response;

namespace CassandraDriver.Results;

/// <summary>
/// A prepared statement
/// </summary>
public class Prepared
{
    /// <summary>
    /// The default constructor for <see cref="Prepared"/>
    /// </summary>
    /// <param name="id">The ID for the prepared statement</param>
    /// <param name="columns">The columns for the prepared statement</param>
    /// <param name="bindMarkers">The bind marekrs for the prepared statement</param>
    public Prepared(byte[] id,
        IReadOnlyList<Column> columns,
        IReadOnlyList<BindMarker> bindMarkers)
    {
        this.Id = id;
        this.Columns = columns;
        this.BindMarkers = bindMarkers;
    }

    /// <summary>
    /// The prepared statements ID
    /// </summary>
    public byte[] Id { get; init; }

    /// <summary>
    /// The columns for the prepared statement
    /// </summary>
    public IReadOnlyList<Column> Columns { get; init; }

    /// <summary>
    /// The bind markers for the prepared statement
    /// </summary>
    public IReadOnlyList<BindMarker> BindMarkers { get; init; }

    internal static Prepared Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        CqlShortBytes id = CqlShortBytes.Deserialize(ref bytes);
        if (id.Bytes is null)
        {
            throw new CassandraException("Got null bytes when trying to get prepared ID");
        }

        CqlPrepareFlags flags =
            (CqlPrepareFlags)BinaryPrimitives.ReadInt32BigEndian(bytes);
        bytes = bytes[sizeof(CqlPrepareFlags)..];

        int bindCount = BinaryPrimitives.ReadInt32BigEndian(bytes);
        bytes = bytes[sizeof(int)..];

        int pkCount = BinaryPrimitives.ReadInt32BigEndian(bytes);
        bytes = bytes[sizeof(int)..];

        HashSet<short> pkIndexes = new(pkCount);
        for (int i = 0; i < pkCount; i++)
        {
            short pkIndex = BinaryPrimitives.ReadInt16BigEndian(bytes);
            bytes = bytes[sizeof(short)..];
            pkIndexes.Add(pkIndex);
        }

        CqlString? globalKeyspace = null;
        CqlString? globalTable = null;
        if ((flags & CqlPrepareFlags.GlobalTableSpec) != 0)
        {
            globalKeyspace = CqlString.Deserialize(ref bytes);
            globalTable = CqlString.Deserialize(ref bytes);
        }

        List<BindMarker> bindMarkers = new(bindCount);
        for (int i = 0; i < bindCount; i++)
        {
            CqlString? keyspace = null;
            CqlString? table = null;
            if ((flags & CqlPrepareFlags.GlobalTableSpec) == 0)
            {
                keyspace = CqlString.Deserialize(ref bytes);
                table = CqlString.Deserialize(ref bytes);
            }

            CqlString name = CqlString.Deserialize(ref bytes);
            ColumnValueType type =
                (ColumnValueType)BinaryPrimitives.ReadInt32BigEndian(bytes);
            bytes = bytes[sizeof(ColumnValueType)..];

            if (!pkIndexes.TryGetValue((short)i, out short index))
            {
                index = -1;
            }

            bindMarkers.Add(new BindMarker(
                    name.Value,
                    type,
                    index,
                    (flags & CqlPrepareFlags.GlobalTableSpec) != 0
                        ? globalKeyspace!.Value
                        : keyspace!.Value,
                    (flags & CqlPrepareFlags.GlobalTableSpec) != 0
                        ? globalTable!.Value
                        : table!.Value
                )
            );
        }

        IReadOnlyList<Column> columns = Column.DeseralizeColumns(ref bytes);

        return new Prepared(id.Bytes.ToArray(), columns, bindMarkers);
    }
}
