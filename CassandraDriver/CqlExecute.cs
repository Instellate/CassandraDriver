using System.Collections.Generic;
using System.Linq;
using CassandraDriver.Frames;
using CassandraDriver.Frames.Request;
using CommunityToolkit.HighPerformance;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver;

internal class CqlExecute
{
    public CqlShortBytes Id { get; set; }
    public CqlConsistency Consistency { get; set; } = CqlConsistency.Any;
    public CqlQueryFlags Flags { get; set; } = CqlQueryFlags.None;
    public CqlParameters? Parameters { get; set; }

    public CqlExecute(byte[] id, object[] objects, CqlConsistency consistency)
    {
        this.Id = new CqlShortBytes(id.ToList());
        Consistency = consistency;
        if (objects.Length <= 0)
        {
            return;
        }

        this.Parameters = new CqlParameters(objects);
        this.Flags = CqlQueryFlags.Values;
    }

    public CqlExecute(byte[] id, CqlLongString query, Dictionary<string, object> dict)
    {
        this.Id = new CqlShortBytes(id.ToList());
        if (dict.Values.Count == 0)
        {
            return;
        }

        this.Parameters = new CqlParameters(dict);
        this.Flags = CqlQueryFlags.Values | CqlQueryFlags.WithNamesForValues;
    }

    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        this.Id.Serialize(writer);
        writer.WriteShort((short)this.Consistency);
        writer.Write((byte)this.Flags);
        if (this.Parameters is null || this.Parameters.Parameters.Count <= 0)
        {
            return;
        }

        this.Parameters.Serialize(writer);
    }

    public int SizeOf()
    {
        int size = Id.SizeOf();
        size += sizeof(CqlConsistency) + sizeof(CqlQueryFlags);
        if (this.Parameters?.Parameters.Count <= 0)
        {
            return size;
        }

        size += sizeof(short);
        size += this.Parameters!.SizeOf();

        return size;
    }
}
