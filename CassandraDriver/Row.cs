using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;
using CassandraDriver.Frames.Response;

namespace CassandraDriver;

public class Row : Dictionary<string, object>
{
    public Row(int reserved) : base(reserved)
    {
    }

    public Row()
    {
    }

    internal static Row Deserialize(ref ReadOnlySpan<byte> bytes, List<CqlColumn> columns)
    {
        Row row = new(columns.Count);

        foreach (CqlColumn column in columns)
        {
            int length = BinaryPrimitives.ReadInt32BigEndian(bytes);
            bytes = bytes[sizeof(int)..];
            object? value = null;

            switch (column.Type)
            {
                case CqlColumnValueType.Custom:
                    break;
                case CqlColumnValueType.Ascii:
                    value = Encoding.ASCII.GetString(bytes.Slice(0, length));
                    bytes = bytes[length..];
                    break;
                case CqlColumnValueType.Bigint:
                    value = BinaryPrimitives.ReadInt64BigEndian(bytes);
                    bytes = bytes[sizeof(long)..];
                    break;
                case CqlColumnValueType.Blob:
                    value = bytes[..length].ToArray();
                    bytes = bytes[length..];
                    break;
                case CqlColumnValueType.Boolean:
                    value = bytes[0] != 0;
                    bytes = bytes[sizeof(byte)..];
                    break;
                case CqlColumnValueType.Counter:
                    value = BinaryPrimitives.ReadInt32BigEndian(bytes);
                    bytes = bytes[sizeof(int)..];
                    break;
                case CqlColumnValueType.Decimal:
                    break;
                case CqlColumnValueType.Double:
                    value = BinaryPrimitives.ReadDoubleBigEndian(bytes);
                    bytes = bytes[sizeof(double)..];
                    break;
                case CqlColumnValueType.Float:
                    break;
                case CqlColumnValueType.Int:
                    value = BinaryPrimitives.ReadInt32BigEndian(bytes);
                    bytes = bytes[sizeof(int)..];
                    break;
                case CqlColumnValueType.Timestamp:
                    break;
                case CqlColumnValueType.Uuid:
                    break;
                case CqlColumnValueType.Varchar:
                    value = Encoding.UTF8.GetString(bytes.Slice(0, length));
                    bytes = bytes[length..];
                    break;
                case CqlColumnValueType.Varint:
                    break;
                case CqlColumnValueType.Timeuuid:
                    break;
                case CqlColumnValueType.Inet:
                    break;
                case CqlColumnValueType.Date:
                    break;
                case CqlColumnValueType.Time:
                    break;
                case CqlColumnValueType.Smallint:
                    break;
                case CqlColumnValueType.Tinyint:
                    break;
                case CqlColumnValueType.List:
                    break;
                case CqlColumnValueType.Map:
                    break;
                case CqlColumnValueType.Set:
                    break;
                case CqlColumnValueType.Udt:
                    break;
                case CqlColumnValueType.Tuple:
                    break;
                default:
                    throw new ArgumentOutOfRangeException(column.Type.ToString());
            }

            if (value is null)
            {
                throw new NotImplementedException();
            }

            row.Add(column.Name.Value, value);
        }

        return row;
    }
}
