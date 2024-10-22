using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Linq;
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
                return this.Bytes.Length;
            }
        }
    }

    public byte[]? Bytes { get; set; }


    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        writer.WriteInt(this.Length);
        if (this.Bytes is not null)
        {
            writer.Write(this.Bytes.ToArray());
        }
    }

    public static CqlBytes Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        int length = BinaryPrimitives.ReadInt32BigEndian(bytes);
        bytes = bytes[sizeof(int)..];
        if (length <= -1)
        {
            return new CqlBytes();
        }

        CqlBytes cqlBytes = new()
        {
            Bytes = bytes[..length].ToArray()
        };
        bytes = bytes[length..];
        return cqlBytes;
    }

    public int SizeOf() => sizeof(int) + this.Bytes?.Length ?? 0;
}
