namespace DomeMQ;

public readonly record struct ConnectBody(string ClientName, byte ProtocolVersion, string AuthToken);

public readonly record struct HelloBody(
    string ServerName,
    byte ProtocolVersion,
    string SessionId,
    uint MaxPayloadSize,
    uint MaxQueueLength,
    byte DefaultChannelType);

public readonly record struct CreateChannelBody(string Name, byte ChannelType, uint MaxQueueLength, uint MaxPayloadSize);

public readonly record struct DeleteChannelBody(string Name);

public readonly record struct PushBody(string ChannelName, uint MaxPayloadSize, string ContentType, byte[] Body);

public readonly record struct PopBody(string ChannelName, uint MaxMessages, uint TimeoutMilliseconds, uint MaxPayloadSize);

public readonly record struct Delivery(string ContentType, byte[] Body);

public readonly record struct CreateChannelAck(string Name, byte ChannelType, uint MaxQueueLength, uint MaxPayloadSize);

public readonly record struct DeleteChannelAck(string Name);

public readonly record struct PushAck(string Name, uint Depth);

public readonly record struct PopAck(string Name, Delivery[] Messages);

public readonly record struct ErrorBody(ushort Code, string Message, byte RelatedType);

public readonly record struct PingBody(long UnixTimeMilliseconds);

public readonly record struct PongBody(long EchoedUnixTimeMilliseconds, long ServerUnixTimeMilliseconds);

public readonly record struct DisconnectBody(ushort ReasonCode, string Reason);

/// <summary>
/// Version-1 control-message bodies. Integers are little-endian and fixed width.
/// Strings are <c>uint16</c> byte-length plus UTF-8. Byte arrays are <c>uint32</c> byte-length plus raw bytes.
/// Decoders read the fields known to version 1 and ignore leftover bytes inside the frame body.
/// </summary>
public static class MessageCodec
{
    public static byte[] EncodeConnect(in ConnectBody body)
    {
        var buffer = new byte[ProtocolEncoding.StringSize(body.ClientName) + 1 + ProtocolEncoding.StringSize(body.AuthToken)];
        var writer = new SpanWriter(buffer);
        writer.WriteString(body.ClientName, ProtocolConstants.MaxClientNameBytes);
        writer.WriteUInt8(body.ProtocolVersion);
        writer.WriteString(body.AuthToken, ProtocolConstants.MaxAuthTokenBytes);
        return buffer;
    }

    public static ConnectBody DecodeConnect(ReadOnlySpan<byte> payload)
    {
        var reader = new SpanReader(payload);
        var name = reader.ReadString(ProtocolConstants.MaxClientNameBytes);
        var version = reader.ReadUInt8();
        var token = reader.ReadString(ProtocolConstants.MaxAuthTokenBytes);
        return new ConnectBody(name, version, token);
    }

    public static byte[] EncodeHello(in HelloBody body)
    {
        var buffer = new byte[
            ProtocolEncoding.StringSize(body.ServerName)
            + 1
            + ProtocolEncoding.StringSize(body.SessionId)
            + 4 + 4 + 1];
        var writer = new SpanWriter(buffer);
        writer.WriteString(body.ServerName, ProtocolConstants.MaxServerNameBytes);
        writer.WriteUInt8(body.ProtocolVersion);
        writer.WriteString(body.SessionId, ProtocolConstants.MaxSessionIdBytes);
        writer.WriteUInt32(body.MaxPayloadSize);
        writer.WriteUInt32(body.MaxQueueLength);
        writer.WriteUInt8(body.DefaultChannelType);
        return buffer;
    }

    public static HelloBody DecodeHello(ReadOnlySpan<byte> payload)
    {
        var reader = new SpanReader(payload);
        return new HelloBody(
            reader.ReadString(ProtocolConstants.MaxServerNameBytes),
            reader.ReadUInt8(),
            reader.ReadString(ProtocolConstants.MaxSessionIdBytes),
            reader.ReadUInt32(),
            reader.ReadUInt32(),
            reader.ReadUInt8());
    }

    public static byte[] EncodeCreateChannel(in CreateChannelBody body)
    {
        var buffer = new byte[ProtocolEncoding.StringSize(body.Name) + 1 + 4 + 4];
        var writer = new SpanWriter(buffer);
        writer.WriteString(body.Name, ProtocolConstants.MaxChannelNameBytes);
        writer.WriteUInt8(body.ChannelType);
        writer.WriteUInt32(body.MaxQueueLength);
        writer.WriteUInt32(body.MaxPayloadSize);
        return buffer;
    }

    public static CreateChannelBody DecodeCreateChannel(ReadOnlySpan<byte> payload)
    {
        var reader = new SpanReader(payload);
        return new CreateChannelBody(
            reader.ReadString(ProtocolConstants.MaxChannelNameBytes),
            reader.ReadUInt8(),
            reader.ReadUInt32(),
            reader.ReadUInt32());
    }

    public static byte[] EncodeDeleteChannel(in DeleteChannelBody body)
    {
        var buffer = new byte[ProtocolEncoding.StringSize(body.Name)];
        var writer = new SpanWriter(buffer);
        writer.WriteString(body.Name, ProtocolConstants.MaxChannelNameBytes);
        return buffer;
    }

    public static DeleteChannelBody DecodeDeleteChannel(ReadOnlySpan<byte> payload)
    {
        var reader = new SpanReader(payload);
        return new DeleteChannelBody(reader.ReadString(ProtocolConstants.MaxChannelNameBytes));
    }

    public static byte[] EncodePush(in PushBody body)
    {
        var buffer = new byte[
            ProtocolEncoding.StringSize(body.ChannelName)
            + 4
            + ProtocolEncoding.StringSize(body.ContentType)
            + 4
            + body.Body.Length];
        var writer = new SpanWriter(buffer);
        writer.WriteString(body.ChannelName, ProtocolConstants.MaxChannelNameBytes);
        writer.WriteUInt32(body.MaxPayloadSize);
        writer.WriteString(body.ContentType, ProtocolConstants.MaxContentTypeBytes);
        writer.WriteByteArray(body.Body);
        return buffer;
    }

    public static PushBody DecodePush(ReadOnlySpan<byte> payload)
    {
        var reader = new SpanReader(payload);
        return new PushBody(
            reader.ReadString(ProtocolConstants.MaxChannelNameBytes),
            reader.ReadUInt32(),
            reader.ReadString(ProtocolConstants.MaxContentTypeBytes),
            reader.ReadByteArray(int.MaxValue));
    }

    public static byte[] EncodePop(in PopBody body)
    {
        var buffer = new byte[ProtocolEncoding.StringSize(body.ChannelName) + 4 + 4 + 4];
        var writer = new SpanWriter(buffer);
        writer.WriteString(body.ChannelName, ProtocolConstants.MaxChannelNameBytes);
        writer.WriteUInt32(body.MaxMessages);
        writer.WriteUInt32(body.TimeoutMilliseconds);
        writer.WriteUInt32(body.MaxPayloadSize);
        return buffer;
    }

    public static PopBody DecodePop(ReadOnlySpan<byte> payload)
    {
        var reader = new SpanReader(payload);
        return new PopBody(
            reader.ReadString(ProtocolConstants.MaxChannelNameBytes),
            reader.ReadUInt32(),
            reader.ReadUInt32(),
            reader.ReadUInt32());
    }

    public static byte[] EncodeCreateChannelAck(in CreateChannelAck body)
    {
        var buffer = new byte[1 + ProtocolEncoding.StringSize(body.Name) + 1 + 4 + 4];
        var writer = new SpanWriter(buffer);
        writer.WriteUInt8((byte)MessageType.CreateChannel);
        writer.WriteString(body.Name, ProtocolConstants.MaxChannelNameBytes);
        writer.WriteUInt8(body.ChannelType);
        writer.WriteUInt32(body.MaxQueueLength);
        writer.WriteUInt32(body.MaxPayloadSize);
        return buffer;
    }

    public static CreateChannelAck DecodeCreateChannelAck(ReadOnlySpan<byte> payload)
    {
        var reader = new SpanReader(payload);
        ExpectAck(ref reader, MessageType.CreateChannel);
        return new CreateChannelAck(
            reader.ReadString(ProtocolConstants.MaxChannelNameBytes),
            reader.ReadUInt8(),
            reader.ReadUInt32(),
            reader.ReadUInt32());
    }

    public static byte[] EncodeDeleteChannelAck(in DeleteChannelAck body)
    {
        var buffer = new byte[1 + ProtocolEncoding.StringSize(body.Name)];
        var writer = new SpanWriter(buffer);
        writer.WriteUInt8((byte)MessageType.DeleteChannel);
        writer.WriteString(body.Name, ProtocolConstants.MaxChannelNameBytes);
        return buffer;
    }

    public static DeleteChannelAck DecodeDeleteChannelAck(ReadOnlySpan<byte> payload)
    {
        var reader = new SpanReader(payload);
        ExpectAck(ref reader, MessageType.DeleteChannel);
        return new DeleteChannelAck(reader.ReadString(ProtocolConstants.MaxChannelNameBytes));
    }

    public static byte[] EncodePushAck(in PushAck body)
    {
        var buffer = new byte[1 + ProtocolEncoding.StringSize(body.Name) + 4];
        var writer = new SpanWriter(buffer);
        writer.WriteUInt8((byte)MessageType.Push);
        writer.WriteString(body.Name, ProtocolConstants.MaxChannelNameBytes);
        writer.WriteUInt32(body.Depth);
        return buffer;
    }

    public static PushAck DecodePushAck(ReadOnlySpan<byte> payload)
    {
        var reader = new SpanReader(payload);
        ExpectAck(ref reader, MessageType.Push);
        return new PushAck(reader.ReadString(ProtocolConstants.MaxChannelNameBytes), reader.ReadUInt32());
    }

    public static int MeasurePopAck(string name, ReadOnlySpan<Delivery> messages)
    {
        var size = 1 + ProtocolEncoding.StringSize(name) + 4;
        foreach (var message in messages)
        {
            size += ProtocolEncoding.StringSize(message.ContentType) + 4 + message.Body.Length;
        }

        return size;
    }

    public static byte[] EncodePopAck(in PopAck body)
    {
        var buffer = new byte[MeasurePopAck(body.Name, body.Messages)];
        var writer = new SpanWriter(buffer);
        writer.WriteUInt8((byte)MessageType.Pop);
        writer.WriteString(body.Name, ProtocolConstants.MaxChannelNameBytes);
        writer.WriteUInt32((uint)body.Messages.Length);
        foreach (var message in body.Messages)
        {
            writer.WriteString(message.ContentType, ProtocolConstants.MaxContentTypeBytes);
            writer.WriteByteArray(message.Body);
        }

        return buffer;
    }

    public static PopAck DecodePopAck(ReadOnlySpan<byte> payload)
    {
        var reader = new SpanReader(payload);
        ExpectAck(ref reader, MessageType.Pop);
        var name = reader.ReadString(ProtocolConstants.MaxChannelNameBytes);
        var count = reader.ReadUInt32();
        if (count > ProtocolConstants.MaxPopBatch)
        {
            throw new ProtocolException($"Pop ack contains {count} messages, above the maximum of {ProtocolConstants.MaxPopBatch}.");
        }

        var messages = new Delivery[count];
        for (var i = 0; i < count; i++)
        {
            messages[i] = new Delivery(
                reader.ReadString(ProtocolConstants.MaxContentTypeBytes),
                reader.ReadByteArray(int.MaxValue));
        }

        return new PopAck(name, messages);
    }

    public static byte[] EncodeError(in ErrorBody body)
    {
        var buffer = new byte[2 + ProtocolEncoding.StringSize(body.Message) + 1];
        var writer = new SpanWriter(buffer);
        writer.WriteUInt16(body.Code);
        writer.WriteString(body.Message, ProtocolConstants.MaxErrorMessageBytes);
        writer.WriteUInt8(body.RelatedType);
        return buffer;
    }

    public static ErrorBody DecodeError(ReadOnlySpan<byte> payload)
    {
        var reader = new SpanReader(payload);
        return new ErrorBody(
            reader.ReadUInt16(),
            reader.ReadString(ProtocolConstants.MaxErrorMessageBytes),
            reader.ReadUInt8());
    }

    public static byte[] EncodePing(in PingBody body)
    {
        var buffer = new byte[8];
        var writer = new SpanWriter(buffer);
        writer.WriteInt64(body.UnixTimeMilliseconds);
        return buffer;
    }

    public static PingBody DecodePing(ReadOnlySpan<byte> payload)
    {
        var reader = new SpanReader(payload);
        return new PingBody(reader.ReadInt64());
    }

    public static byte[] EncodePong(in PongBody body)
    {
        var buffer = new byte[16];
        var writer = new SpanWriter(buffer);
        writer.WriteInt64(body.EchoedUnixTimeMilliseconds);
        writer.WriteInt64(body.ServerUnixTimeMilliseconds);
        return buffer;
    }

    public static PongBody DecodePong(ReadOnlySpan<byte> payload)
    {
        var reader = new SpanReader(payload);
        return new PongBody(reader.ReadInt64(), reader.ReadInt64());
    }

    public static byte[] EncodeDisconnect(in DisconnectBody body)
    {
        var buffer = new byte[2 + ProtocolEncoding.StringSize(body.Reason)];
        var writer = new SpanWriter(buffer);
        writer.WriteUInt16(body.ReasonCode);
        writer.WriteString(body.Reason, ProtocolConstants.MaxReasonBytes);
        return buffer;
    }

    public static DisconnectBody DecodeDisconnect(ReadOnlySpan<byte> payload)
    {
        var reader = new SpanReader(payload);
        return new DisconnectBody(reader.ReadUInt16(), reader.ReadString(ProtocolConstants.MaxReasonBytes));
    }

    private static void ExpectAck(ref SpanReader reader, MessageType expected)
    {
        var actual = reader.ReadUInt8();
        if (actual != (byte)expected)
        {
            throw new ProtocolException($"Ack type {actual} does not match expected {(byte)expected}.");
        }
    }
}
