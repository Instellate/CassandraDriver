using System.Collections.Generic;
using System.Linq;
using CommunityToolkit.HighPerformance;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames.Request;

internal class CqlQuery : ICqlSerializable
{
    public CqlLongString Query { get; set; }
    public CqlConsistency Consistency { get; set; } = CqlConsistency.Any;
    public CqlQueryFlags Flags { get; set; } = CqlQueryFlags.None;
    public CqlParameters? Parameters { get; set; }

    public CqlQuery(CqlLongString query, object[] objects, CqlConsistency consistency)
    {
        this.Query = query;
        this.Consistency = consistency;
        if (objects.Length <= 0)
        {
            return;
        }

        this.Parameters = new CqlParameters(objects);
        this.Flags = CqlQueryFlags.Values;
    }

    public CqlQuery(CqlLongString query, Dictionary<string, object> dict)
    {
        this.Query = query;
        if (dict.Values.Count == 0)
        {
            return;
        }

        this.Parameters = new CqlParameters(dict);

        this.Flags = CqlQueryFlags.Values | CqlQueryFlags.WithNamesForValues;
    }

    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        this.Query.Serialize(writer);
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
        int size = this.Query.SizeOf();
        size += sizeof(CqlConsistency) + sizeof(CqlQueryFlags);
        if (!(this.Parameters?.Parameters.Count > 0))
        {
            return size;
        }

        size += sizeof(short);
        size += this.Parameters?.Parameters
            .Select((kp) => kp.Key?.SizeOf() ?? 0 + kp.Value.SizeOf())
            .Sum() ?? 0;

        return size;
    }
}
