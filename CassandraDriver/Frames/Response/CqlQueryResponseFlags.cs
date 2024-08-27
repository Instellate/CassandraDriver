using System;

namespace CassandraDriver.Frames.Response;

[Flags]
internal enum CqlQueryResponseFlags
{
    GlobalTableSpec = 0x01,
    HasMorePages = 0x02,
    NoMetadata = 0x04,
}
