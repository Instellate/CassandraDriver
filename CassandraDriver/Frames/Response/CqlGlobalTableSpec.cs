using System;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames.Response;

internal class CqlGlobalTableSpec : ICqlSerializable
{
    public required CqlString Keyspace { get; init; }
    public required CqlString Table { get; init; }

    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        this.Keyspace.Serialize(writer);
        this.Table.Serialize(writer);
    }

    public static CqlGlobalTableSpec Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        return new()
        {
            Keyspace = CqlString.Deserialize(ref bytes),
            Table = CqlString.Deserialize(ref bytes),
        };
    }

    public int SizeOf() => this.Keyspace.SizeOf() + this.Table.SizeOf();
}
