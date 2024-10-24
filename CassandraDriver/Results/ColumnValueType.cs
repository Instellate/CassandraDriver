namespace CassandraDriver.Results;

/// <summary>
/// Enum for all the types a column can be. Read more [here](https://opensource.docs.scylladb.com/stable/cql/types.html) for more indebt information
/// </summary>
public enum ColumnValueType : short
{
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
    Custom = 0x00,
    Ascii = 0x01,
    Bigint = 0x02,
    Blob = 0x03,
    Boolean = 0x04,
    Counter = 0x05,
    Decimal = 0x06,
    Double = 0x07,
    Float = 0x08,
    Int = 0x09,
    Timestamp = 0x0B,
    Uuid = 0x0C,
    Varchar = 0x0D,
    Varint = 0x0E,
    Timeuuid = 0x0F,
    Inet = 0x10,
    Date = 0x11,
    Time = 0x12,
    Smallint = 0x13,
    Tinyint = 0x14,
    List = 0x20,
    Map = 0x21,
    Set = 0x22,
    Udt = 0x30,
    Tuple = 0x31,
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
}
