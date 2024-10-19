namespace CassandraDriver.Frames;

internal enum CqlVersion : byte
{
    Request = 0x04,
    Response = 0x84
}
