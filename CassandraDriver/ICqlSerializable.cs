using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver;

internal interface ICqlSerializable
{
    public void Serialize(ArrayPoolBufferWriter<byte> writer);

    public int SizeOf();
}
