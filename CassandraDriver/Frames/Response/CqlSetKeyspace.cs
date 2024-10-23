using System;
using System.Collections.Generic;
using System.Threading;
using CassandraDriver.Results;

namespace CassandraDriver.Frames.Response;

internal class CqlSetKeyspace : Query
{
    internal CqlSetKeyspace(string keyspace)
    {
        this.SetKeyspace = keyspace;
    }

    public override IReadOnlyList<Row> LocalRows =>
        throw new CassandraException("Set keyspace does not implement rows");

    public override IReadOnlyList<Column> Columns =>
        throw new CassandraException("Set keyspace does not implement rows");

    public override Row this[int index] =>
        throw new CassandraException("Cannot index on query type \"Set keyspace\"");

    public override int Count => 0;

    public override byte[]? PagingState =>
        throw new CassandraException(
            "Cannot get paging state on query on query type \"Set keyspace\"");

    public override IAsyncEnumerator<Row>
        GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
        throw new CassandraException(
            "Cannot async iterate with query type \"Set keyspace\"");

    public static CqlSetKeyspace Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        CqlString keyspace = CqlString.Deserialize(ref bytes);
        return new CqlSetKeyspace(keyspace.Value);
    }
}
