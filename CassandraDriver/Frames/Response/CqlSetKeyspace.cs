using System;
using System.Collections.Generic;
using CassandraDriver.Results;

namespace CassandraDriver.Frames.Response;

internal class CqlSetKeyspace : Query
{
    internal CqlSetKeyspace(string keyspace)
    {
        this.SetKeyspace = keyspace;
    }

    public override IReadOnlyList<Row> Rows =>
        throw new CassandraException("Set keyspace does not implement rows");

    public override Row this[int index] =>
        throw new CassandraException("Cannot index on query type \"Set keyspace\"");

    public override int Count => 0;

    public static CqlSetKeyspace Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        CqlString keyspace = CqlString.Deserialize(ref bytes);
        return new CqlSetKeyspace(keyspace.Value);
    }
}
