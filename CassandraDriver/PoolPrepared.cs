using System.Collections.Concurrent;
using CassandraDriver.Results;

namespace CassandraDriver;

internal sealed class PoolPrepared : Prepared
{
    public ConcurrentDictionary<string, byte[]> NodeIds { get; set; } = [];

    public PoolPrepared(Prepared prepared)
        : base(prepared.Id, prepared.Columns, prepared.BindMarkers)
    {
    }
}
