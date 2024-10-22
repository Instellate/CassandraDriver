using System.Collections.Generic;
using System.Linq;
using CassandraDriver.Frames;
using CassandraDriver.Frames.Request;
using CommunityToolkit.HighPerformance;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver;

internal class CqlExecute
{
    private readonly CqlShortBytes _id;
    private readonly CqlConsistency _consistency = CqlConsistency.Any;
    private readonly CqlQueryFlags _flags = CqlQueryFlags.None;
    private readonly CqlParameters? _parameters;
    private readonly CqlBytes? _pagingState;
    private readonly int? _itemsPerPages;

    public CqlExecute(byte[] id,
        object[] objects,
        CqlConsistency consistency,
        bool skipMetadata = false,
        byte[]? pagingState = null,
        int? itemsPerPages = null)
    {
        this._id = new CqlShortBytes(id.ToList());
        this._consistency = consistency;

        if (skipMetadata)
        {
            this._flags |= CqlQueryFlags.SkipMetadata;
        }

        if (pagingState is not null)
        {
            this._pagingState = new CqlBytes
            {
                Bytes = pagingState
            };
            this._flags |= CqlQueryFlags.WithPagingState;
        }

        this._itemsPerPages = itemsPerPages;
        if (itemsPerPages is not null)
        {
            this._flags |= CqlQueryFlags.PageSize;
        }

        if (objects.Length <= 0)
        {
            return;
        }

        this._parameters = new CqlParameters(objects);
        this._flags |= CqlQueryFlags.Values;
    }

    public CqlExecute(byte[] id, Dictionary<string, object> dict)
    {
        this._id = new CqlShortBytes(id.ToList());
        if (dict.Values.Count == 0)
        {
            return;
        }

        this._parameters = new CqlParameters(dict);
        this._flags = CqlQueryFlags.Values | CqlQueryFlags.WithNamesForValues;
    }

    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        this._id.Serialize(writer);
        writer.WriteShort((short)this._consistency);
        writer.Write((byte)this._flags);

        if (this._parameters is not null && this._parameters.Parameters.Count > 0)
        {
            this._parameters.Serialize(writer);
        }

        if (this._itemsPerPages is { } value)
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
        int size = this._id.SizeOf();
        size += sizeof(CqlConsistency) + sizeof(CqlQueryFlags);

        if (this._itemsPerPages is not null)
        {
            size += sizeof(int);
        }

        if (this._pagingState is not null)
        {
            size += this._pagingState.SizeOf();
        }

        if ((this._parameters?.Parameters.Count ?? 0) > 0)
        {
            size += sizeof(short);
            size += this._parameters!.SizeOf();
        }

        return size;
    }
}
