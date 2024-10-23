using System.Collections.Generic;
using System.Linq;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames;

internal class CqlParameters : ICqlSerializable
{
    private readonly bool _isNamedValues;

    public List<KeyValuePair<CqlString?, CqlValue>> Parameters { get; set; }

    public CqlParameters(object?[] objects)
    {
        this.Parameters
            = new List<KeyValuePair<CqlString?, CqlValue>>(objects.Length);
        foreach (object? value in objects)
        {
            this.Parameters.Add(
                new KeyValuePair<CqlString?, CqlValue>(
                    null,
                    CqlValue.CreateCqlValue(value)
                )
            );
        }

        this._isNamedValues = false;
    }

    public CqlParameters(Dictionary<string, object> dict)
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

        this._isNamedValues = true;
    }


    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        writer.WriteShort((short)this.Parameters.Count);
        foreach ((CqlString? key, CqlValue value) in this.Parameters)
        {
            if (this._isNamedValues && key is not null)
            {
                key.Serialize(writer);
            }

            value.Serialize(writer);
        }
    }

    public int SizeOf()
    {
        return this.Parameters
            .Select((kp) => kp.Key?.SizeOf() ?? 0 + kp.Value.SizeOf())
            .Sum();
    }
}
