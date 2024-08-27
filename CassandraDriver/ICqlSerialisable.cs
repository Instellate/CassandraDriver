using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver;

public interface ICqlSerializable
{
    public void Serialize(ArrayPoolBufferWriter<byte> writer);

    public int SizeOf();
}
