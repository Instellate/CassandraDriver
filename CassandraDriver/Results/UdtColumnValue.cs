using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using CassandraDriver.Frames;

namespace CassandraDriver.Results;

public class UdtColumnValue : ColumnValue
{
    public required IReadOnlyList<Column> UdtColumns { get; init; }
    public required string TypeName { get; init; }
    public required string Keyspace { get; init; }


    public new static ColumnValue Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        CqlString keyspace = CqlString.Deserialize(ref bytes);
        CqlString typeName = CqlString.Deserialize(ref bytes);

        short fieldCount = BinaryPrimitives.ReadInt16BigEndian(bytes);
        bytes = bytes[sizeof(short)..];

        List<Column> columns = new(fieldCount);
        for (short i = 0; i < fieldCount; i++)
        {
            Column column = Column.Deserialize(ref bytes, false);
            columns.Add(column);
        }

        return new UdtColumnValue()
        {
            Type = ColumnValueType.Udt,
            TypeName = typeName.Value,
            UdtColumns = columns,
            Keyspace = keyspace.Value
        };
    }
}
