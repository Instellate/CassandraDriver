using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames;

internal struct CqlValue : ICqlSerializable
{
    internal readonly byte[] Bytes;

    public CqlValue(byte[] bytes)
    {
        this.Bytes = bytes;
    }

    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        writer.WriteInt(this.Bytes.Length);
        writer.Write(this.Bytes);
    }

    public static CqlValue CreateCqlValue(object value)
    {
        byte[] bytes;
        switch (value)
        {
            case string s:
                return new CqlValue(Encoding.UTF8.GetBytes(s));
            case int i:
                bytes = new byte[sizeof(int)];
                BinaryPrimitives.WriteInt32BigEndian(bytes, i);
                return new CqlValue(bytes);
            case long l:
                bytes = new byte[sizeof(long)];
                BinaryPrimitives.WriteInt64BigEndian(bytes, l);
                return new CqlValue(bytes);
            case short sh:
                bytes = new byte[sizeof(short)];
                BinaryPrimitives.WriteInt16BigEndian(bytes, sh);
                return new CqlValue(bytes);
            case DateTimeOffset dto:
                bytes = new byte[8];
                BinaryPrimitives.WriteInt64BigEndian(bytes, dto.ToUnixTimeMilliseconds());
                return new CqlValue(bytes);
            case byte[] bt:
                return new CqlValue(bt);
            case Guid id:
                return new CqlValue(id.ToByteArray(true));
            case TimeSpan ts:
                bytes = new byte[8];
                BinaryPrimitives.WriteInt64BigEndian(bytes, (long)ts.TotalNanoseconds);
                return new CqlValue(bytes);
            case sbyte sb:
                return new CqlValue([(byte)sb]);
            case float f:
                bytes = new byte[sizeof(float)];
                BinaryPrimitives.WriteSingleBigEndian(bytes, f);
                return new CqlValue(bytes);
            case double d:
                bytes = new byte[sizeof(double)];
                BinaryPrimitives.WriteDoubleBigEndian(bytes, d);
                return new CqlValue(bytes);
            case bool b:
                return new CqlValue([(byte)(b ? 1 : 0)]);
            default:
                throw new CassandraException(
                    $"Couldn't convert type {value.GetType().Name} to CqlValue"
                );
        }
    }

    public int SizeOf() => sizeof(int) + this.Bytes.Length;
}
