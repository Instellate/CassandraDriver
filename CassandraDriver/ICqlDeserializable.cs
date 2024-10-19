using CassandraDriver.Results;

namespace CassandraDriver;

/// <summary>
/// A class used for deserialization of rows
/// </summary>
/// <typeparam name="T">The type that inherits the interface</typeparam>
public interface ICqlDeserializable<T>
{
    /// <summary>
    /// Deserilises the row into class {T}
    /// </summary>
    /// <param name="row">The row to the deserialize</param>
    /// <returns>The deserialized class</returns>
    public static abstract T DeserializeRow(Row row);
}
