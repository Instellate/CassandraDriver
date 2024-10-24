using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames;

internal class CqlStringList : ICqlSerializable
{
    public List<CqlString> Strings { get; set; }

    public CqlStringList(List<string> strings)
    {
        this.Strings = new List<CqlString>(strings.Count);
        foreach (string str in strings)
        {
            this.Strings.Add(str);
        }
    }

    public CqlStringList(List<CqlString> strings)
    {
        this.Strings = strings;
    }

    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        writer.WriteShort((short)this.Strings.Count);
        foreach (CqlString s in this.Strings)
        {
            s.Serialize(writer);
        }
    }

    public int SizeOf() => sizeof(short) + this.Strings.Select(s => s.SizeOf()).Sum();

    public static CqlStringList Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        short size = BinaryPrimitives.ReadInt16BigEndian(bytes);
        bytes = bytes[sizeof(short)..];
        List<CqlString> strings = new(size);
        for (short i = 0; i < size; i++)
        {
            strings.Add(CqlString.Deserialize(ref bytes));
        }

        return new CqlStringList(strings);
    }
}
