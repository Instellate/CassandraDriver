using CassandraDriver.Results;

namespace CassandraDriver;

public interface ICqlDeserializable<T>
{
    public static abstract T DeserializeRow(Row row);
}
