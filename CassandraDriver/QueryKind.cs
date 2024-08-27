namespace CassandraDriver;

public enum QueryKind
{
    Void = 0x01,
    Rows = 0x02,
    SetKeyspace = 0x03,
    Prepared = 0x04,
    SchemeChange = 0x05,
}
