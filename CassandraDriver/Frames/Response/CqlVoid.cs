using System.Collections.Generic;
using CassandraDriver.Results;

namespace CassandraDriver.Frames.Response;

internal class CqlVoid : Query
{
    public static CqlVoid Instance = new();


    public override IReadOnlyList<Row> Rows =>
        throw new CassandraException("Void does not implement rows");

    internal CqlVoid()
    {
        this.Kind = QueryKind.Void;
    }

    public override Row this[int index] =>
        throw new CassandraException("Cannot index on query type void");

    public override int Count => 0;
}
