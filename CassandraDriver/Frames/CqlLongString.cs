using System;
using System.Buffers.Binary;
using System.Text;
using CommunityToolkit.HighPerformance.Buffers;

namespace CassandraDriver.Frames;

public sealed class CqlLongString : ICqlSerializable
{
    private string _value;

    public CqlLongString(string value)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            value.Length,
            int.MaxValue,
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
                int.MaxValue,
                "Value"
            );

            this._value = value;
        }
    }

    public void Serialize(ArrayPoolBufferWriter<byte> writer)
    {
        int byteCount = Encoding.UTF8.GetByteCount(this._value);
        writer.WriteInt(byteCount);
        Encoding.UTF8.GetBytes(
            this._value,
            writer.GetSpan(byteCount)
        );
        writer.Advance(byteCount);
    }

    public int SizeOf()
    {
        return sizeof(int) + Encoding.UTF8.GetByteCount(this._value);
    }

    public static CqlLongString Deserialize(ref ReadOnlySpan<byte> bytes)
    {
        int size = BinaryPrimitives.ReadInt32BigEndian(bytes);
        CqlLongString str = new(Encoding.UTF8.GetString(bytes.Slice(sizeof(int), size)));
        bytes = bytes[(sizeof(int) + size)..];
        return str;
    }

    public static implicit operator CqlLongString(string str)
    {
        return new CqlLongString(str);
    }
}
