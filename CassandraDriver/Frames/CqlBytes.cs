using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using CommunityToolkit.HighPerformance;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames;

internal class CqlBytes : ICqlSerializable
{
    public int Length
    {
        get
        {
            if (this.Bytes is null)
            {
                return -1;
            }
            else
            {
                return this.Bytes.Count;
            }
        }
    }

    public List<byte>? Bytes { get; set; }


    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        writer.WriteInt(this.Length);
        if (this.Bytes is not null)
        {
            writer.Write(this.Bytes.AsSpan());
        }
    }

    public static CqlBytes Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        int length = BinaryPrimitives.ReadInt32BigEndian(bytes);
        bytes = bytes[sizeof(int)..];

        CqlBytes cqlBytes = new()
        {
            Bytes = new List<byte>(length)
        };

        for (int i = 0; i < length; i++)
        {
            cqlBytes.Bytes.Add(bytes[i]);
        }

        bytes = bytes[length..];
        return cqlBytes;
    }

    public int SizeOf() => sizeof(int) + this.Bytes?.Count ?? 0;
}
