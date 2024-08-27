namespace CassandraDriver.Frames;

public enum CqlConsistency : short
{
    Any = 0x00,
    One = 0x01,
    Two = 0x02,
    Three = 0x03,
    Quorum = 0x04,
    All = 0x05,
    LocalQuorum = 0x06,
    EachQuorum = 0x07,
    Serial = 0x08,
    LocalSerial = 0x09,
    LocalOne = 0x0A,
}
