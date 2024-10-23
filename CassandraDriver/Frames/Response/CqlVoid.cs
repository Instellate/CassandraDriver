using System.Collections.Generic;
using System.Threading;
using CassandraDriver.Results;

namespace CassandraDriver.Frames.Response;

internal class CqlVoid : Query
{
    public static readonly CqlVoid Instance = new();

    public override IReadOnlyList<Row> LocalRows =>
        throw new CassandraException("Void does not implement rows");

    public override IReadOnlyList<Column> Columns =>
        throw new CassandraException("Void does not implement columns");

    internal CqlVoid()
    {
        this.Kind = QueryKind.Void;
    }

    public override Row this[int index] =>
        throw new CassandraException("Cannot index on query type void");

    public override int Count => 0;

    public override byte[] PagingState =>
        throw new CassandraException("Void does not implement paging state");

    public override IAsyncEnumerator<Row>
        GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
        throw new CassandraException("Void does not implement async iterators");
}
