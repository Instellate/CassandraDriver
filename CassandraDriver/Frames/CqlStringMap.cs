using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames;

public sealed class CqlStringMap : Dictionary<CqlString, CqlString>, ICqlSerializable
{
    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(this.Values.Count, short.MaxValue);

        writer.WriteShort((short)this.Values.Count);
        foreach ((CqlString key, CqlString value) in this)
        {
            key.Serialize(writer);
            value.Serialize(writer);
        }
    }

    public int SizeOf()
    {
        return sizeof(short)
               + this.Select(pair => pair.Key.SizeOf() + pair.Value.SizeOf()).Sum();
    }

    public static CqlStringMap Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        short size = BinaryPrimitives.ReadInt16BigEndian(bytes);
        CqlStringMap values = new();

        bytes = bytes[2..];
        for (short i = 0; i < size; i++)
        {
            CqlString key = CqlString.Deserialize(ref bytes);
            CqlString value = CqlString.Deserialize(ref bytes);
            values.Add(key, value);
        }

        return values;
    }
}
