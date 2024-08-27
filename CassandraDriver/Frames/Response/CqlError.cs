using System;
using System.Buffers.Binary;

namespace CassandraDriver.Frames.Response;

internal sealed class CqlError
{
    public int Code { get; private set; }
    public CqlString Message { get; private set; }

    /// <summary>
    /// Represents error from server.
    /// This class should only be initialised through <see cref="Deserialize"/>
    /// </summary>
    private CqlError()
    {
        this.Message = null!;
    }

    public static CqlError Deserialize(ReadOnlySpan<byte> bytes)
    {
        CqlError error = new()
        {
            Code = BinaryPrimitives.ReadInt32BigEndian(bytes)
        };
        bytes = bytes[sizeof(int)..];
        error.Message = CqlString.Deserialize(ref bytes);

        return error;
    }
}
