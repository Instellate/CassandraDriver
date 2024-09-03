using System;

namespace CassandraDriver.Frames.Response;

[Flags]
internal enum CqlPrepareFlags
{
    GlobalTableSpec = 0x01,
}
