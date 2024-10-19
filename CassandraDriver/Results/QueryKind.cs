namespace CassandraDriver.Results;

/// <summary>
/// The kind of query that was returned by the database
/// </summary>
public enum QueryKind
{
    /// <summary>
    /// Nothing has been returned
    /// </summary>
    Void = 0x01,

    /// <summary>
    /// Rows has been returned
    /// </summary>
    Rows = 0x02,

    /// <summary>
    /// The new keyspace has been set
    /// </summary>
    SetKeyspace = 0x03,

    /// <summary>
    /// Prepared statement has been returned
    /// </summary>
    Prepared = 0x04,

    /// <summary>
    /// The scheme has been changed
    /// </summary>
    SchemeChange = 0x05,
}
