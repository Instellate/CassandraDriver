using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using CommunityToolkit.HighPerformance;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames;

internal class CqlShortBytes : ICqlSerializable
{
    public CqlShortBytes(List<byte>? bytes)
    {
        this.Bytes = bytes;
    }

    public short Length
    {
        get
        {
            if (this.Bytes is null)
            {
                return -1;
            }
            else
            {
                return (short)this.Bytes.Count;
            }
        }
    }

    public List<byte>? Bytes { get; set; }


    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        writer.WriteShort(this.Length);
        if (this.Bytes is not null)
        {
            writer.Write(this.Bytes.AsSpan());
        }
    }

    public static CqlShortBytes Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        short length = BinaryPrimitives.ReadInt16BigEndian(bytes);
        bytes = bytes[sizeof(short)..];

        CqlShortBytes cqlBytes = new(new List<byte>(length));

        for (short i = 0; i < length; i++)
        {
            cqlBytes.Bytes!.Add(bytes[i]);
        }

        bytes = bytes[length..];

        return cqlBytes;
    }

    public int SizeOf() => sizeof(short) + this.Bytes?.Count ?? 0;
}
