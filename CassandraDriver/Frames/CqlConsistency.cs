namespace CassandraDriver.Frames;

/// <summary>
/// The consistency to use for a query. On what to use read about [consistency](https://opensource.docs.scylladb.com/stable/cql/consistency.html) here
/// </summary>
public enum CqlConsistency : short
{
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
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
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
}
