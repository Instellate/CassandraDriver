using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Numerics;
using System.Reflection;
using System.Text;
using CassandraDriver.Frames.Response;

namespace CassandraDriver;

public class Row : Dictionary<string, object>
{
    private Row(int reserved) : base(reserved)
    {
    }


    static Row()
    {
        Dictionary<CqlColumnValueType, CreateSetDelegate> setDict
            = new(DataTypeTypes.Count);
        Dictionary<CqlColumnValueType, CreateListDelegate> listDict
            = new(DataTypeTypes.Count);

        foreach ((CqlColumnValueType key, Type value) in DataTypeTypes)
        {
            setDict.Add(
                key,
                ExpressionBuilder.CreateDelegate<CreateSetDelegate>(
                    value,
                    typeof(HashSet<>),
                    nameof(HashSet<int>.Add)
                )
            );
            listDict.Add(
                key,
                ExpressionBuilder.CreateDelegate<CreateListDelegate>(
                    value,
                    typeof(HashSet<>),
                    nameof(HashSet<int>.Add)
                )
            );
        }

        CreateSetFuncs = setDict.ToFrozenDictionary();
        CreateListFuncs = listDict.ToFrozenDictionary();
    }

    private delegate object CreateSetDelegate(
        CqlColumnValue value,
        int length,
        ref ReadOnlySpan<byte> bytes
    );

    private delegate object CreateListDelegate(
        CqlColumnValue value,
        int length,
        ref ReadOnlySpan<byte> bytes
    );

    private static readonly FrozenDictionary<CqlColumnValueType, Type>
        DataTypeTypes = new Dictionary<CqlColumnValueType, Type>()
            {
                { CqlColumnValueType.Ascii, typeof(string) },
                { CqlColumnValueType.Bigint, typeof(long) },
                { CqlColumnValueType.Blob, typeof(byte[]) },
                { CqlColumnValueType.Boolean, typeof(bool) },
                { CqlColumnValueType.Counter, typeof(int) },
                { CqlColumnValueType.Double, typeof(double) },
                { CqlColumnValueType.Int, typeof(int) },
                { CqlColumnValueType.Varchar, typeof(string) },
                { CqlColumnValueType.Smallint, typeof(short) },
                { CqlColumnValueType.Timestamp, typeof(DateTimeOffset) },
                { CqlColumnValueType.Uuid, typeof(Guid) },
                { CqlColumnValueType.Time, typeof(TimeSpan) },
                { CqlColumnValueType.Inet, typeof(IPAddress) },
                { CqlColumnValueType.Float, typeof(float) },
                { CqlColumnValueType.Smallint, typeof(short) },
                { CqlColumnValueType.Tinyint, typeof(byte) },
                { CqlColumnValueType.Date, typeof(DateTime) },
            }
            .ToFrozenDictionary();

    private static readonly FrozenDictionary<CqlColumnValueType, CreateSetDelegate>
        CreateSetFuncs;

    private static readonly FrozenDictionary<CqlColumnValueType, CreateListDelegate>
        CreateListFuncs;


    internal static Row Deserialize(ref ReadOnlySpan<byte> bytes, List<CqlColumn> columns)
    {
        Row row = new(columns.Count);

        foreach (CqlColumn column in columns)
        {
            row.Add(column.Name.Value, ParseDataTypes(column.Value, ref bytes));
        }

        return row;
    }

    internal static object ParseDataTypes(
        CqlColumnValue column,
        ref ReadOnlySpan<byte> bytes
    )
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
                value = BinaryPrimitives.ReadSingleBigEndian(bytes);
                bytes = bytes[sizeof(float)..];
                break;
            case CqlColumnValueType.Int:
                value = BinaryPrimitives.ReadInt32BigEndian(bytes);
                bytes = bytes[sizeof(int)..];
                break;
            case CqlColumnValueType.Timestamp:
                value = DateTimeOffset.FromUnixTimeMilliseconds(
                    BinaryPrimitives.ReadInt64BigEndian(bytes)
                );
                bytes = bytes[sizeof(long)..];
                break;
            case CqlColumnValueType.Uuid:
                byte[] guidBytes
                    = ((BigInteger)BinaryPrimitives.ReadInt128BigEndian(bytes))
                    .ToByteArray();
                value = new Guid(guidBytes);
                bytes = bytes[16..];
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
                if (length == 4)
                {
                    value = new IPAddress(BinaryPrimitives.ReadUInt32BigEndian(bytes));
                    bytes = bytes[sizeof(uint)..];
                }
                else
                {
                    value = new IPAddress(
                        ((BigInteger)BinaryPrimitives.ReadInt128BigEndian(bytes))
                        .ToByteArray()
                    );
                    bytes = bytes[16..];
                }

                break;
            case CqlColumnValueType.Date:
                value = new DateTime(-5877641, 06, 23)
                    .AddDays(BinaryPrimitives.ReadUInt32BigEndian(bytes));
                bytes = bytes[length..];
                break;
            case CqlColumnValueType.Time:
                value = new TimeSpan(BinaryPrimitives.ReadInt64BigEndian(bytes) * 100);
                bytes = bytes[sizeof(long)..];
                break;
            case CqlColumnValueType.Smallint:
                value = BinaryPrimitives.ReadInt16BigEndian(bytes);
                bytes = bytes[sizeof(short)..];
                break;
            case CqlColumnValueType.Tinyint:
                value = bytes[0];
                bytes = bytes[sizeof(byte)..];
                break;
            case CqlColumnValueType.List:
                int listCount = BinaryPrimitives.ReadInt32BigEndian(bytes);
                bytes = bytes[sizeof(int)..];
                value = CreateListFuncs[column.AdditionalType!.Type]
                    .Invoke(column.AdditionalType!, listCount, ref bytes);
                break;
            case CqlColumnValueType.Map:
                break;
            case CqlColumnValueType.Set:
                int setCount = BinaryPrimitives.ReadInt32BigEndian(bytes);
                bytes = bytes[sizeof(int)..];
                value = CreateSetFuncs[column.AdditionalType!.Type]
                    .Invoke(column.AdditionalType!, setCount, ref bytes);
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

        return value;
    }
}
