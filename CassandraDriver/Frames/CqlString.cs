using System;
using System.Buffers.Binary;
using System.Text;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames;

public sealed class CqlString : ICqlSerializable
{
    private string _value;

    public CqlString(string value)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            value.Length,
            short.MaxValue,
            "value"
        );

        this._value = value;
    }

    public string Value
    {
        get => this._value;
        set
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThan(
                value.Length,
                short.MaxValue,
                "Value"
            );

            this._value = value;
        }
    }

    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        short byteCount = (short)Encoding.UTF8.GetByteCount(this._value);
        writer.WriteShort(byteCount);
        Encoding.UTF8.GetBytes(
            this._value,
            writer.GetSpan(byteCount)
        );
        writer.Advance(byteCount);
    }

    public int SizeOf()
    {
        return sizeof(short) + Encoding.UTF8.GetByteCount(this._value);
    }

    public static CqlString Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        short size = BinaryPrimitives.ReadInt16BigEndian(bytes);
        string str = Encoding.UTF8.GetString(bytes.Slice(sizeof(short), size));
        bytes = bytes[(sizeof(short) + size)..];
        return new CqlString(str);
    }

    public static implicit operator CqlString(string str)
    {
        return new CqlString(str);
    }
}
