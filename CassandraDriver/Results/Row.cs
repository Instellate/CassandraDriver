using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Net;
using System.Numerics;
using System.Text;

namespace CassandraDriver.Results;

public sealed class Row : Dictionary<string, object?>
{
    private Row(int reserved) : base(reserved)
    {
    }

    static Row()
    {
        Dictionary<ColumnValueType, CreateSetDelegate> setDict
            = new(DataTypeTypes.Count);
        Dictionary<ColumnValueType, CreateListDelegate> listDict
            = new(DataTypeTypes.Count);

        foreach ((ColumnValueType key, Type value) in DataTypeTypes)
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
                    typeof(List<>),
                    nameof(List<int>.Add)
                )
            );
        }

        CreateSetFuncs = setDict.ToFrozenDictionary();
        CreateListFuncs = listDict.ToFrozenDictionary();
    }

    private delegate object CreateSetDelegate(
        ColumnValue value,
        int length,
        ref ReadOnlySpan<byte> bytes
    );

    private delegate object CreateListDelegate(
        ColumnValue value,
        int length,
        ref ReadOnlySpan<byte> bytes
    );

    private delegate object CreateMapDelegate(ColumnValue keyColumn,
        ColumnValue valueColumn,
        int length,
        ref ReadOnlySpan<byte> bytes);

    public static readonly FrozenDictionary<ColumnValueType, Type>
        DataTypeTypes = new Dictionary<ColumnValueType, Type>()
            {
                { ColumnValueType.Ascii, typeof(string) },
                { ColumnValueType.Bigint, typeof(long) },
                { ColumnValueType.Blob, typeof(byte[]) },
                { ColumnValueType.Boolean, typeof(bool) },
                { ColumnValueType.Counter, typeof(int) },
                { ColumnValueType.Double, typeof(double) },
                { ColumnValueType.Int, typeof(int) },
                { ColumnValueType.Varchar, typeof(string) },
                { ColumnValueType.Smallint, typeof(short) },
                { ColumnValueType.Timestamp, typeof(DateTimeOffset) },
                { ColumnValueType.Uuid, typeof(Guid) },
                { ColumnValueType.Time, typeof(TimeSpan) },
                { ColumnValueType.Inet, typeof(IPAddress) },
                { ColumnValueType.Float, typeof(float) },
                { ColumnValueType.Tinyint, typeof(byte) },
                { ColumnValueType.Date, typeof(DateTime) },
                { ColumnValueType.Udt, typeof(Row) },
                { ColumnValueType.Varint, typeof(BigInteger) },
                { ColumnValueType.Tuple, typeof(List<object?>) }
            }
            .ToFrozenDictionary();

    private static readonly FrozenDictionary<ColumnValueType, CreateSetDelegate>
        CreateSetFuncs;

    private static readonly FrozenDictionary<ColumnValueType, CreateListDelegate>
        CreateListFuncs;

    private static readonly
        ConcurrentDictionary<(ColumnValueType, ColumnValueType), CreateMapDelegate>
        CreateMapDelegates = [];

    private static CreateMapDelegate GetCreateMapDelegate(ColumnValueType key,
        ColumnValueType value)
    {
        if (CreateMapDelegates.TryGetValue((key, value),
                out CreateMapDelegate? mapDelegate))
        {
            return mapDelegate;
        }
        else
        {
            CreateMapDelegate newDelegate
                = ExpressionBuilder.CreateMapDelegate<CreateMapDelegate>(
                    DataTypeTypes[key],
                    DataTypeTypes[value]);
            CreateMapDelegates.TryAdd((key, value), newDelegate);
            return newDelegate;
        }
    }


    internal static Row Deserialize(ref ReadOnlySpan<byte> bytes,
        IReadOnlyList<Column> columns)
    {
        Row row = new(columns.Count);

        foreach (Column column in columns)
        {
            row.Add(column.Name, ParseDataTypes(column.Value, ref bytes));
        }

        return row;
    }

    internal static object? ParseDataTypes(
        ColumnValue column,
        ref ReadOnlySpan<byte> bytes
    )
    {
        int length = BinaryPrimitives.ReadInt32BigEndian(bytes);
        bytes = bytes[sizeof(int)..];
        if (length < 0)
        {
            return null;
        }

        object value;

        switch (column.Type)
        {
            case ColumnValueType.Custom:
                throw
                    new NotImplementedException(); // I'll ignore this, I don't know what the usage is for this, and genuinely seems like a pain to implement
            case ColumnValueType.Ascii:
                value = Encoding.ASCII.GetString(bytes.Slice(0, length));
                bytes = bytes[length..];
                break;
            case ColumnValueType.Bigint:
                value = BinaryPrimitives.ReadInt64BigEndian(bytes);
                bytes = bytes[sizeof(long)..];
                break;
            case ColumnValueType.Blob:
                value = bytes[..length].ToArray();
                bytes = bytes[length..];
                break;
            case ColumnValueType.Boolean:
                value = bytes[0] != 0;
                bytes = bytes[sizeof(byte)..];
                break;
            case ColumnValueType.Counter:
                value = BinaryPrimitives.ReadInt32BigEndian(bytes);
                bytes = bytes[sizeof(int)..];
                break;
            case ColumnValueType.Decimal:
                int scale = BinaryPrimitives.ReadInt32BigEndian(bytes);
                bytes = bytes[sizeof(int)..];
                BigInteger unscaled = new(bytes[..(length - sizeof(int))], false, true);
                decimal dec = (decimal)unscaled;
                value = dec * (decimal)Math.Pow(10, -1 * scale);
                bytes = bytes[(length - sizeof(int))..];
                break;
            case ColumnValueType.Double:
                value = BinaryPrimitives.ReadDoubleBigEndian(bytes);
                bytes = bytes[sizeof(double)..];
                break;
            case ColumnValueType.Float:
                value = BinaryPrimitives.ReadSingleBigEndian(bytes);
                bytes = bytes[sizeof(float)..];
                break;
            case ColumnValueType.Int:
                value = BinaryPrimitives.ReadInt32BigEndian(bytes);
                bytes = bytes[sizeof(int)..];
                break;
            case ColumnValueType.Timestamp:
                value = DateTimeOffset.FromUnixTimeMilliseconds(
                    BinaryPrimitives.ReadInt64BigEndian(bytes)
                );
                bytes = bytes[sizeof(long)..];
                break;
            case ColumnValueType.Uuid:
                Span<byte> guidBytes = stackalloc byte[16];
                bytes[..16].CopyTo(guidBytes);
                value = new Guid(guidBytes, true);
                bytes = bytes[16..];
                break;
            case ColumnValueType.Varchar:
                value = Encoding.UTF8.GetString(bytes.Slice(0, length));
                bytes = bytes[length..];
                break;
            case ColumnValueType.Varint:
                value = new BigInteger(bytes[..length], false, true);
                bytes = bytes[length..];
                break;
            case ColumnValueType.Timeuuid:
                Span<byte> timeUuid = stackalloc byte[16];
                bytes.CopyTo(timeUuid);
                timeUuid.Reverse();
                value = new Guid(timeUuid);
                bytes = bytes[16..];
                break;
            case ColumnValueType.Inet:
                value = new IPAddress(bytes[..length]);
                bytes = bytes[length..];
                break;
            case ColumnValueType.Date:
                value = new DateTime(1970, 1, 1)
                    .AddDays(
                        BinaryPrimitives.ReadUInt32BigEndian(bytes)
                        - 2147483648D
                    ); // The constant is 2^31, value for where cassandra native protocol centers epoch
                bytes = bytes[sizeof(uint)..];
                break;
            case ColumnValueType.Time:
                value = new TimeSpan(BinaryPrimitives.ReadInt64BigEndian(bytes) * 100);
                bytes = bytes[sizeof(long)..];
                break;
            case ColumnValueType.Smallint:
                value = BinaryPrimitives.ReadInt16BigEndian(bytes);
                bytes = bytes[sizeof(short)..];
                break;
            case ColumnValueType.Tinyint:
                value = (sbyte)bytes[0];
                bytes = bytes[sizeof(sbyte)..];
                break;
            case ColumnValueType.List:
                int listCount = BinaryPrimitives.ReadInt32BigEndian(bytes);
                bytes = bytes[sizeof(int)..];
                value = CreateListFuncs[column.AdditionalType!.Type]
                    .Invoke(column.AdditionalType!, listCount, ref bytes);
                break;
            case ColumnValueType.Map:
                int mapCount = BinaryPrimitives.ReadInt32BigEndian(bytes);
                bytes = bytes[sizeof(int)..];
                value = GetCreateMapDelegate(column.AdditionalType!.Type,
                    column.AdditionalType2!.Type).Invoke(column.AdditionalType,
                    column.AdditionalType2,
                    mapCount,
                    ref bytes);
                break;
            case ColumnValueType.Set:
                int setCount = BinaryPrimitives.ReadInt32BigEndian(bytes);
                bytes = bytes[sizeof(int)..];
                value = CreateSetFuncs[column.AdditionalType!.Type]
                    .Invoke(column.AdditionalType!, setCount, ref bytes);
                break;
            case ColumnValueType.Udt:
                if (column is UdtColumnValue udt)
                {
                    value = Deserialize(ref bytes, udt.UdtColumns);
                }
                else
                {
                    throw new CassandraException("Expected UDT, got normal column value");
                }

                break;
            case ColumnValueType.Tuple:
                if (column is TupleColumnValue tuple)
                {
                    List<object?> tupleList = new(tuple.Types.Count);
                    foreach (ColumnValue columnValue in tuple.Types)
                    {
                        tupleList.Add(ParseDataTypes(columnValue, ref bytes));
                    }

                    value = tupleList;
                }
                else
                {
                    throw new CassandraException(
                        "Expected tuple, got normal column value");
                }

                break;
            default:
                throw new ArgumentOutOfRangeException(column.Type.ToString());
        }

        return value;
    }
}
