using System;

namespace CassandraDriver.Frames.Request;

[Flags]
public enum CqlQueryFlags : byte
{
    None = 0x00,
    Values = 0x01,
    SkipMetadata = 0x02,
    PageSize = 0x04,
    WithPagingState = 0x08,
    WithSerialConsistency = 0x010,
    WithDefaultTimestamp = 0x20,
    WithNamesForValues = 0x40,
}
