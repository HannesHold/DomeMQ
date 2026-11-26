namespace DomeMQ;

/// <summary>A decoded 16-byte header. <see cref="PayloadLength"/> is the body size, not including the header.</summary>
public readonly record struct FrameHeader(
    byte Version,
    byte Type,
    ushort Flags,
    uint RequestId,
    uint PayloadLength);

/// <summary>One protocol frame: header fields plus the raw body.</summary>
public readonly record struct Frame(
    byte Version,
    byte Type,
    ushort Flags,
    uint RequestId,
    ReadOnlyMemory<byte> Payload)
{
    public bool TryGetMessageType(out MessageType type)
    {
        if (Enum.IsDefined(typeof(MessageType), Type))
        {
            type = (MessageType)Type;
            return true;
        }

        type = default;
        return false;
    }
}

/// <summary>Encodes and decodes the fixed 16-byte little-endian header.</summary>
public static class FrameCodec
{
    public static Frame Create(MessageType type, uint requestId, ReadOnlyMemory<byte> payload, ushort flags = FrameFlags.None) =>
        new(ProtocolConstants.Version, (byte)type, flags, requestId, payload);

    public static byte[] Encode(in Frame frame)
    {
        var payload = frame.Payload.Span;
        var buffer = new byte[ProtocolConstants.HeaderSize + payload.Length];
        Encode(frame, buffer);
        return buffer;
    }

    public static int Encode(in Frame frame, Span<byte> destination)
    {
        var payload = frame.Payload.Span;
        var total = ProtocolConstants.HeaderSize + payload.Length;
        if (destination.Length < total)
        {
            throw new ArgumentException("Destination is smaller than the frame.", nameof(destination));
        }

        ProtocolConstants.Magic.CopyTo(destination);
        destination[4] = frame.Version;
        destination[5] = frame.Type;
        BinaryPrimitives.WriteUInt16LittleEndian(destination[6..], frame.Flags);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[8..], frame.RequestId);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[12..], (uint)payload.Length);
        payload.CopyTo(destination[ProtocolConstants.HeaderSize..]);
        return total;
    }

    public static bool TryReadHeader(ReadOnlySpan<byte> header, out FrameHeader value, out string error)
    {
        value = default;
        if (header.Length < ProtocolConstants.HeaderSize)
        {
            error = "Header is shorter than 16 bytes.";
            return false;
        }

        if (!header[..4].SequenceEqual(ProtocolConstants.Magic))
        {
            error = "Magic bytes do not match DMQ protocol.";
            return false;
        }

        value = new FrameHeader(
            header[4],
            header[5],
            BinaryPrimitives.ReadUInt16LittleEndian(header[6..]),
            BinaryPrimitives.ReadUInt32LittleEndian(header[8..]),
            BinaryPrimitives.ReadUInt32LittleEndian(header[12..]));
        error = "";
        return true;
    }
}
