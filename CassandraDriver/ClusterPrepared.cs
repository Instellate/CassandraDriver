using System.Collections.Concurrent;
using CassandraDriver.Results;

namespace CassandraDriver;

internal sealed class ClusterPrepared : Prepared
{
    public ConcurrentDictionary<string, byte[]> NodeIds { get; set; } = [];

    public ClusterPrepared(Prepared prepared)
        : base(prepared.Id, prepared.Columns, prepared.BindMarkers)
    {
    }
}
