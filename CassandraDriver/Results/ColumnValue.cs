using System;
using System.Buffers.Binary;

namespace CassandraDriver.Results;

/// <summary>
/// Values related to columns
/// </summary>
public class ColumnValue
{
    internal ColumnValue()
    {
    }

    /// <summary>
    /// The type of the column
    /// </summary>
    public required ColumnValueType Type { get; init; }

    /// <summary>
    /// Additional type
    /// </summary>
    /// <remarks>This should be migrated into it's own class</remarks>
    public ColumnValue? AdditionalType { get; init; }

    /// <summary>
    /// Additional type2
    /// </summary>
    /// <remarks>This should be migrated into it's own class</remarks>
    public ColumnValue? AdditionalType2 { get; init; }

    internal static ColumnValue Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        ColumnValueType type =
            (ColumnValueType)BinaryPrimitives.ReadInt16BigEndian(bytes);
        bytes = bytes[sizeof(short)..];

        ColumnValue? additionalType = null;
        ColumnValue? additionalType2 = null;
        switch (type)
        {
            case ColumnValueType.List:
                additionalType = Deserialize(ref bytes);
                break;
            case ColumnValueType.Set:
                additionalType = Deserialize(ref bytes);
                break;
            case ColumnValueType.Map:
                additionalType = Deserialize(ref bytes);
                additionalType2 = Deserialize(ref bytes);
                break;
            case ColumnValueType.Udt:
                return UdtColumnValue.Deserialize(ref bytes);
            case ColumnValueType.Tuple:
                return TupleColumnValue.Deserialize(ref bytes);
        }

        return new ColumnValue()
        {
            Type = type,
            AdditionalType = additionalType,
            AdditionalType2 = additionalType2
        };
    }
}
