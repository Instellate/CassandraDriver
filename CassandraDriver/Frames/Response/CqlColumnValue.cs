using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Security.Cryptography;

namespace CassandraDriver.Frames.Response;

internal class CqlColumnValue
{
    public required CqlColumnValueType Type { get; init; }
    public CqlColumnValue? AdditionalType { get; init; }
    public CqlColumnValue? AdditionalType2 { get; init; }

    public static CqlColumnValue Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        CqlColumnValueType type =
            (CqlColumnValueType)BinaryPrimitives.ReadInt16BigEndian(bytes);
        bytes = bytes[sizeof(short)..];

        CqlColumnValue? additionalType = null;
        CqlColumnValue? additionalType2 = null;
        switch (type)
        {
            case CqlColumnValueType.List:
                additionalType = Deserialize(ref bytes);
                break;
            case CqlColumnValueType.Set:
                additionalType = Deserialize(ref bytes);
                break;
            case CqlColumnValueType.Map:
                additionalType = Deserialize(ref bytes);
                additionalType2 = Deserialize(ref bytes);
                break;
        }

        return new CqlColumnValue()
        {
            Type = type,
            AdditionalType = additionalType,
            AdditionalType2 = additionalType2
        };
    }
}
