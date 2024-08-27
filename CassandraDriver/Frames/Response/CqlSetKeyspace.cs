using System;

namespace CassandraDriver.Frames.Response;

internal class CqlSetKeyspace : Query
{
    internal CqlSetKeyspace(string keyspace)
    {
        this.SetKeyspace = keyspace;
    }

    public override Row this[int index]
        => throw new CassandraException("Cannot index on query type \"Set keyspace\"");

    public static CqlSetKeyspace Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        CqlString keyspace = CqlString.Deserialize(ref bytes);
        return new CqlSetKeyspace(keyspace.Value);
    }
}
