using System;
using System.Buffers.Binary;

namespace CassandraDriver.Results;

public class ColumnValue
{
    internal ColumnValue()
    {
    }

    public required ColumnValueType Type { get; init; }
    public ColumnValue? AdditionalType { get; init; }
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
