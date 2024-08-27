using System;
using System.Collections.Generic;
using System.Linq;
using CommunityToolkit.HighPerformance;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames.Request;

internal class CqlQuery : ICqlSerializable
{
    private readonly bool _isNamedValues = false;

    public CqlLongString Query { get; set; }
    public CqlConsistency Consistency { get; set; } = CqlConsistency.Any;
    public CqlQueryFlags Flags { get; set; } = CqlQueryFlags.None;
    public List<KeyValuePair<CqlString?, CqlValue>>? Parameters { get; set; }

    public CqlQuery(CqlLongString query, object[] objects, CqlConsistency consistency)
    {
        this.Query = query;
        Consistency = consistency;
        if (objects.Length > 0)
        {
            this.Parameters
                = new List<KeyValuePair<CqlString?, CqlValue>>(objects.Length);
            Flags = CqlQueryFlags.Values;
            foreach (object value in objects)
            {
                this.Parameters.Add(
                    new KeyValuePair<CqlString?, CqlValue>(
                        null,
                        CqlValue.CreateCqlValue(value)
                    )
                );
            }
        }
    }

    public CqlQuery(CqlLongString query, Dictionary<string, object> dict)
    {
        this.Query = query;
        this._isNamedValues = true;
        if (dict.Values.Count != 0)
        {
            this.Parameters
                = new List<KeyValuePair<CqlString?, CqlValue>>(dict.Values.Count);
            foreach ((string key, object value) in dict)
            {
                this.Parameters.Add(
                    new KeyValuePair<CqlString?, CqlValue>(
                        key,
                        CqlValue.CreateCqlValue(value)
                    )
                );
            }

            Flags = CqlQueryFlags.Values | CqlQueryFlags.WithNamesForValues;
        }
    }

    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        this.Query.Serialize(writer);
        writer.WriteShort((short)this.Consistency);
        writer.Write((byte)this.Flags);
        if (Parameters is not null && Parameters.Count > 0)
        {
            writer.WriteShort((short)Parameters.Count);
            foreach ((CqlString? key, CqlValue value) in this.Parameters)
            {
                if (this._isNamedValues && key is not null)
                {
                    key.Serialize(writer);
                }

                value.Serialize(writer);
            }
        }
    }

    public int SizeOf()
    {
        int size = this.Query.SizeOf();
        size += sizeof(CqlConsistency) + sizeof(CqlQueryFlags);
        if (Parameters?.Count > 0)
        {
            size += sizeof(short);
            size += Parameters?.Select((kp) => kp.Key?.SizeOf() ?? 0 + kp.Value.SizeOf())
                .Sum() ?? 0;
            ;
        }

        return size;
    }
}
