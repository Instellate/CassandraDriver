using System.Buffers.Binary;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver;

internal static class ArrayPoolBufferWriterExtensions
{
    public static void WriteShort(this ArrayPoolBufferWriter<byte> writer, short num)
    {
        BinaryPrimitives.WriteInt16BigEndian(writer.GetSpan(sizeof(short)), num);
        writer.Advance(sizeof(short));
    }

    public static void WriteInt(this ArrayPoolBufferWriter<byte> writer, int num)
    {
        BinaryPrimitives.WriteInt32BigEndian(writer.GetSpan(sizeof(int)), num);
        writer.Advance(sizeof(int));
    }
}
