using System;

namespace CassandraDriver.Frames;

[Flags]
public enum CqlFlags : byte
{
    Compression = 0x01,
    Tracing = 0x02,
    CustomPayload = 0x04,
    Warning = 0x08
}
