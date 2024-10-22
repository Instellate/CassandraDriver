using System.Collections.Generic;
using System.Linq;
using CommunityToolkit.HighPerformance;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames.Request;

internal class CqlQuery : ICqlSerializable
{
    private readonly CqlBytes? _pagingState;

    public CqlLongString Query { get; set; }
    public CqlConsistency Consistency { get; set; } = CqlConsistency.Any;
    public CqlQueryFlags Flags { get; set; } = CqlQueryFlags.None;
    public CqlParameters? Parameters { get; set; }
    public byte[]? PagingState => this._pagingState?.Bytes;
    public int? ResultPageSize { get; }

    public CqlQuery(CqlLongString query,
        object[] objects,
        CqlConsistency consistency,
        bool skipMetadata = false,
        byte[]? pagingState = null,
        int? resultPageSize = null)
    {
        if (skipMetadata)
        {
            this.Flags = CqlQueryFlags.SkipMetadata;
        }

        if (pagingState is not null)
        {
            this._pagingState = new CqlBytes()
            {
                Bytes = pagingState
            };
            this.Flags |= CqlQueryFlags.WithPagingState;
        }

        this.Query = query;
        this.ResultPageSize = resultPageSize;
        if (this.ResultPageSize is not null)
        {
            this.Flags |= CqlQueryFlags.PageSize;
        }

        this.Consistency = consistency;
        if (objects.Length <= 0)
        {
            return;
        }

        this.Parameters = new CqlParameters(objects);
        this.Flags |= CqlQueryFlags.Values;
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
        if (this.Parameters is not null && this.Parameters.Parameters.Count > 0)
        {
            this.Parameters.Serialize(writer);
        }

        if (ResultPageSize is { } value)
        {
            writer.WriteInt(value);
        }

        if (this._pagingState is not null)
        {
            this._pagingState.Serialize(writer);
        }
    }

    public int SizeOf()
    {
        int size = this.Query.SizeOf();
        size += sizeof(CqlConsistency) + sizeof(CqlQueryFlags);
        if (this.Parameters?.Parameters.Count > 0)
        {
            size += sizeof(short);
            size += this.Parameters?.Parameters
                .Select((kp) => kp.Key?.SizeOf() ?? 0 + kp.Value.SizeOf())
                .Sum() ?? 0;
        }

        if (ResultPageSize is not null)
        {
            size += sizeof(int);
        }

        if (this._pagingState is not null)
        {
            size += this._pagingState.SizeOf();
        }

        return size;
    }
}
