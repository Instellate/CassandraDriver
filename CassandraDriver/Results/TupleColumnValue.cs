using System;
using System.Buffers.Binary;
using System.Collections.Generic;

namespace CassandraDriver.Results;

/// <summary>
/// The column value for a tuple
/// </summary>
public sealed class TupleColumnValue : ColumnValue
{
    /// <summary>
    /// All the types in the tuple
    /// </summary>
    public required IReadOnlyList<ColumnValue> Types { get; init; }

    internal new static ColumnValue Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        int count = BinaryPrimitives.ReadInt16BigEndian(bytes);
        bytes = bytes[sizeof(short)..];

        List<ColumnValue> types = new(count);
        for (short i = 0; i < count; i++)
        {
            ColumnValue value = ColumnValue.Deserialize(ref bytes);
            types.Add(value);
        }

        return new TupleColumnValue()
        {
            Type = ColumnValueType.Tuple,
            Types = types
        };
    }
}
