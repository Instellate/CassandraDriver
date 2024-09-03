using System;
using CassandraDriver.Frames;

namespace CassandraDriver;

internal sealed class StreamData
{
    public required ReadOnlyMemory<byte> Body { get; init; }
    public required CqlFrame Frame { get; init; }
    public CqlStringList? Warnings { get; init; } = null;
}
