using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames;

public class CqlStringList : ICqlSerializable
{
    public List<CqlString> Strings { get; set; }

    public CqlStringList(List<string> strings)
    {
        Strings = new List<CqlString>(strings.Count);
        foreach (string str in strings)
        {
            Strings.Add(str);
        }
    }

    public CqlStringList(List<CqlString> strings)
    {
        Strings = strings;
    }

    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        writer.WriteShort((short)Strings.Count);
        foreach (CqlString s in this.Strings)
        {
            s.Serialize(writer);
        }
    }

    public int SizeOf() => sizeof(short) + Strings.Select(s => s.SizeOf()).Sum();

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
