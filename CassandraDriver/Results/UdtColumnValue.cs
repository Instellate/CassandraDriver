using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using CassandraDriver.Frames;

namespace CassandraDriver.Results;

/// <summary>
/// The column value for a UDT
/// </summary>
public sealed class UdtColumnValue : ColumnValue
{
    /// <summary>
    /// All the columns in the UDT
    /// </summary>
    public required IReadOnlyList<Column> UdtColumns { get; init; }

    /// <summary>
    /// The name of the UDT
    /// </summary>
    public required string TypeName { get; init; }

    /// <summary>
    /// The keyspace the UDT is in
    /// </summary>
    public required string Keyspace { get; init; }

    internal new static ColumnValue Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        CqlString keyspace = CqlString.Deserialize(ref bytes);
        CqlString typeName = CqlString.Deserialize(ref bytes);

        short fieldCount = BinaryPrimitives.ReadInt16BigEndian(bytes);
        bytes = bytes[sizeof(short)..];

        List<Column> columns = new(fieldCount);
        for (short i = 0; i < fieldCount; i++)
        {
            Column column = Column.Deserialize(ref bytes, false);
            columns.Add(column);
        }

        return new UdtColumnValue()
        {
            Type = ColumnValueType.Udt,
            TypeName = typeName.Value,
            UdtColumns = columns,
            Keyspace = keyspace.Value
        };
    }
}
