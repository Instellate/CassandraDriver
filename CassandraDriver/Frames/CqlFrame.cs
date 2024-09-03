using System;
using System.Buffers.Binary;
using CommunityToolkit.HighPerformance;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames;

internal class CqlFrame : ICqlSerializable
{
    public const int CqlFrameConstSize = sizeof(byte) + sizeof(byte) +
                                         sizeof(short) + sizeof(byte) + sizeof(int);

    public CqlFrame()
    {
    }

    public CqlFrame(short stream, CqlOpCode opCode, int length, CqlFlags flags = 0)
    {
        this.Stream = stream;
        this.OpCode = opCode;
        this.Length = length;
        this.Flags = flags;
        this.Version = CqlVersion.Request;
    }

    public CqlVersion Version { get; set; }
    public CqlFlags Flags { get; set; } = 0;
    public short Stream { get; set; }
    public CqlOpCode OpCode { get; set; }

    public int Length { get; set; }

    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        writer.Write(this.Version);
        writer.Write(this.Flags);
        writer.WriteShort(this.Stream);
        writer.Write(this.OpCode);
        writer.WriteInt(this.Length);
    }

    public static CqlFrame Deserialize(byte[] bytes)
    {
        ReadOnlySpan<byte> span = bytes;

        byte version = span[0];
        span = span[sizeof(byte)..];

        byte flags = span[0];
        span = span[sizeof(byte)..];

        short stream = BinaryPrimitives.ReadInt16BigEndian(span);
        span = span[sizeof(short)..];

        byte opcode = span[0];
        span = span[sizeof(byte)..];

        int length = BinaryPrimitives.ReadInt32BigEndian(span);

        return new CqlFrame()
        {
            Version = (CqlVersion)version,
            Flags = (CqlFlags)flags,
            Stream = stream,
            OpCode = (CqlOpCode)opcode,
            Length = length
        };
    }

    public int SizeOf()
    {
        return CqlFrameConstSize;
    }
}
