using System.Text;

namespace DomeMQ;

internal ref struct SpanWriter
{
    private readonly Span<byte> _buffer;
    private int _position;

    public SpanWriter(Span<byte> buffer)
    {
        _buffer = buffer;
        _position = 0;
    }

    public int Written => _position;

    public void WriteUInt8(byte value)
    {
        Need(1);
        _buffer[_position++] = value;
    }

    public void WriteUInt16(ushort value)
    {
        Need(2);
        BinaryPrimitives.WriteUInt16LittleEndian(_buffer[_position..], value);
        _position += 2;
    }

    public void WriteUInt32(uint value)
    {
        Need(4);
        BinaryPrimitives.WriteUInt32LittleEndian(_buffer[_position..], value);
        _position += 4;
    }

    public void WriteInt64(long value)
    {
        Need(8);
        BinaryPrimitives.WriteInt64LittleEndian(_buffer[_position..], value);
        _position += 8;
    }

    public void WriteBool(bool value) => WriteUInt8(value ? (byte)1 : (byte)0);

    public void WriteString(string value, int maxBytes)
    {
        var byteCount = ProtocolEncoding.Utf8.GetByteCount(value);
        if (byteCount > maxBytes || byteCount > ushort.MaxValue)
        {
            throw new ProtocolException($"String of {byteCount} bytes exceeds the limit of {Math.Min(maxBytes, ushort.MaxValue)}.");
        }

        WriteUInt16((ushort)byteCount);
        Need(byteCount);
        ProtocolEncoding.Utf8.GetBytes(value, _buffer[_position..]);
        _position += byteCount;
    }

    public void WriteByteArray(ReadOnlySpan<byte> value)
    {
        WriteUInt32((uint)value.Length);
        Need(value.Length);
        value.CopyTo(_buffer[_position..]);
        _position += value.Length;
    }

    private void Need(int count)
    {
        if (_position > _buffer.Length - count)
        {
            throw new InvalidOperationException("Encode buffer is too small.");
        }
    }
}

internal ref struct SpanReader
{
    private readonly ReadOnlySpan<byte> _buffer;
    private int _position;

    public SpanReader(ReadOnlySpan<byte> buffer)
    {
        _buffer = buffer;
        _position = 0;
    }

    public int Remaining => _buffer.Length - _position;

    public byte ReadUInt8()
    {
        Need(1);
        return _buffer[_position++];
    }

    public ushort ReadUInt16()
    {
        Need(2);
        var value = BinaryPrimitives.ReadUInt16LittleEndian(_buffer[_position..]);
        _position += 2;
        return value;
    }

    public uint ReadUInt32()
    {
        Need(4);
        var value = BinaryPrimitives.ReadUInt32LittleEndian(_buffer[_position..]);
        _position += 4;
        return value;
    }

    public long ReadInt64()
    {
        Need(8);
        var value = BinaryPrimitives.ReadInt64LittleEndian(_buffer[_position..]);
        _position += 8;
        return value;
    }

    public bool ReadBool()
    {
        var value = ReadUInt8();
        return value switch
        {
            0 => false,
            1 => true,
            _ => throw new ProtocolException($"Invalid boolean value {value}. Expected 0 or 1.")
        };
    }

    public string ReadString(int maxBytes)
    {
        var length = ReadUInt16();
        if (length > maxBytes)
        {
            throw new ProtocolException($"String length {length} exceeds the limit of {maxBytes}.");
        }

        Need(length);
        var slice = _buffer.Slice(_position, length);
        _position += length;
        try
        {
            return ProtocolEncoding.Utf8.GetString(slice);
        }
        catch (DecoderFallbackException)
        {
            throw new ProtocolException("String is not valid UTF-8.");
        }
    }

    public byte[] ReadByteArray(int maxLength)
    {
        var length = ReadUInt32();
        if (length > (uint)maxLength)
        {
            throw new ProtocolException($"Byte array length {length} exceeds the limit of {maxLength}.");
        }

        Need((int)length);
        var bytes = _buffer.Slice(_position, (int)length).ToArray();
        _position += (int)length;
        return bytes;
    }

    private void Need(int count)
    {
        if (count < 0 || _position > _buffer.Length - count)
        {
            throw new ProtocolException("Truncated message body.");
        }
    }
}

internal static class ProtocolEncoding
{
    public static readonly UTF8Encoding Utf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static int StringSize(string value) => 2 + Utf8.GetByteCount(value);
}
