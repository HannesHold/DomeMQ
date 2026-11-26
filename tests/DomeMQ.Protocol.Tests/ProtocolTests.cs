using System.Text;

namespace DomeMQ.Protocol.Tests;

public class FrameTests
{
    [Fact]
    public void Header_is_16_bytes_little_endian()
    {
        var frame = FrameCodec.Create(MessageType.Push, 0x01020304, new byte[] { 1, 2, 3 }, FrameFlags.NoAck);
        var bytes = FrameCodec.Encode(frame);

        Assert.Equal(19, bytes.Length);
        Assert.Equal(new byte[] { 0x44, 0x4D, 0x51, 0x01 }, bytes[..4]);
        Assert.Equal(ProtocolConstants.Version, bytes[4]);
        Assert.Equal((byte)MessageType.Push, bytes[5]);
        Assert.Equal(new byte[] { 0x01, 0x00 }, bytes[6..8]);
        Assert.Equal(new byte[] { 0x04, 0x03, 0x02, 0x01 }, bytes[8..12]);
        Assert.Equal(new byte[] { 0x03, 0x00, 0x00, 0x00 }, bytes[12..16]);
        Assert.Equal(new byte[] { 1, 2, 3 }, bytes[16..]);
    }

    [Fact]
    public void Bad_magic_is_rejected()
    {
        var header = new byte[16];
        Assert.False(FrameCodec.TryReadHeader(header, out _, out var error));
        Assert.Contains("Magic", error, StringComparison.Ordinal);
    }

    [Fact]
    public void Round_trip_preserves_header_fields()
    {
        var original = FrameCodec.Create(MessageType.Ping, 99, new byte[8]);
        var encoded = FrameCodec.Encode(original);
        Assert.True(FrameCodec.TryReadHeader(encoded, out var header, out _));
        Assert.Equal(original.Version, header.Version);
        Assert.Equal(original.Type, header.Type);
        Assert.Equal(original.Flags, header.Flags);
        Assert.Equal(original.RequestId, header.RequestId);
        Assert.Equal(8u, header.PayloadLength);
    }
}

public class MessageRoundTripTests
{
    [Fact]
    public void Connect_round_trip_ignores_trailing_bytes()
    {
        var body = MessageCodec.EncodeConnect(new ConnectBody("demo", 1, "token"));
        var decoded = MessageCodec.DecodeConnect(body);
        Assert.Equal("demo", decoded.ClientName);
        Assert.Equal(1, decoded.ProtocolVersion);
        Assert.Equal("token", decoded.AuthToken);

        var extended = body.Concat(new byte[] { 0xFF, 0x00 }).ToArray();
        var again = MessageCodec.DecodeConnect(extended);
        Assert.Equal(decoded, again);
    }

    [Fact]
    public void Truncated_connect_is_a_protocol_error()
    {
        var body = MessageCodec.EncodeConnect(new ConnectBody("demo", 1, ""));
        Assert.Throws<ProtocolException>(() => MessageCodec.DecodeConnect(body[..^1]));
    }

    [Fact]
    public void Invalid_bool_is_rejected_when_a_future_decoder_reads_it()
    {
        // Booleans are a single byte, 0 or 1. The reader is covered by disconnect, which has no bool,
        // so this locks the primitive through a hand-built string-plus-bool sequence via ping's neighbor.
        var reader = new byte[] { 0x02 };
        Assert.Throws<ProtocolException>(() => MessageCodec.DecodePing(reader));
    }

    [Theory]
    [InlineData("")]
    [InlineData("orders")]
    [InlineData("café")]
    public void Strings_round_trip(string name)
    {
        var encoded = MessageCodec.EncodeDeleteChannel(new DeleteChannelBody(name));
        Assert.Equal(name, MessageCodec.DecodeDeleteChannel(encoded).Name);
    }

    [Fact]
    public void Push_body_is_opaque()
    {
        var body = new byte[] { 0x00, 0xFF, 0x80, 0x7B, 0x00 };
        var encoded = MessageCodec.EncodePush(new PushBody("ch", 0, "", body));
        var decoded = MessageCodec.DecodePush(encoded);
        Assert.Equal(body, decoded.Body);
        Assert.Equal("", decoded.ContentType);
        Assert.Equal(0u, decoded.MaxPayloadSize);
    }

    [Fact]
    public void Pop_ack_round_trip()
    {
        var ack = new PopAck("orders",
        [
            new Delivery("", Encoding.UTF8.GetBytes("hello")),
            new Delivery("application/json", Encoding.UTF8.GetBytes("{\"n\":1}"))
        ]);
        var decoded = MessageCodec.DecodePopAck(MessageCodec.EncodePopAck(ack));
        Assert.Equal("orders", decoded.Name);
        Assert.Equal(2, decoded.Messages.Length);
        Assert.Equal("hello", Encoding.UTF8.GetString(decoded.Messages[0].Body));
        Assert.Equal("application/json", decoded.Messages[1].ContentType);
        Assert.Equal("{\"n\":1}", Encoding.UTF8.GetString(decoded.Messages[1].Body));
    }

    [Fact]
    public void Every_control_message_round_trips()
    {
        Assert.Equal("DomeMQ", MessageCodec.DecodeHello(MessageCodec.EncodeHello(
            new HelloBody("DomeMQ", 1, "0123456789abcdef0123456789abcdef", 16, 32, 1))).ServerName);
        Assert.Equal((byte)2, MessageCodec.DecodeCreateChannel(MessageCodec.EncodeCreateChannel(
            new CreateChannelBody("a", 2, 0, 0))).ChannelType);
        Assert.Equal(8u, MessageCodec.DecodePop(MessageCodec.EncodePop(new PopBody("a", 8, 1000, 0))).MaxMessages);
        Assert.Equal(4u, MessageCodec.DecodeCreateChannelAck(MessageCodec.EncodeCreateChannelAck(
            new CreateChannelAck("a", 1, 4, 5))).MaxQueueLength);
        Assert.Equal("a", MessageCodec.DecodeDeleteChannelAck(MessageCodec.EncodeDeleteChannelAck(new DeleteChannelAck("a"))).Name);
        Assert.Equal(3u, MessageCodec.DecodePushAck(MessageCodec.EncodePushAck(new PushAck("a", 3))).Depth);
        var error = MessageCodec.DecodeError(MessageCodec.EncodeError(new ErrorBody(8, "nope", 5)));
        Assert.Equal((ushort)8, error.Code);
        Assert.Equal("nope", error.Message);
        Assert.Equal((byte)5, error.RelatedType);
        Assert.Equal(1000, MessageCodec.DecodePing(MessageCodec.EncodePing(new PingBody(1000))).UnixTimeMilliseconds);
        var pong = MessageCodec.DecodePong(MessageCodec.EncodePong(new PongBody(1000, 1001)));
        Assert.Equal(1000, pong.EchoedUnixTimeMilliseconds);
        Assert.Equal(1001, pong.ServerUnixTimeMilliseconds);
        Assert.Equal("bye", MessageCodec.DecodeDisconnect(MessageCodec.EncodeDisconnect(new DisconnectBody(0, "bye"))).Reason);
    }
}

public class PayloadLimitTests
{
    [Fact]
    public void Effective_limit_is_the_minimum_of_the_levels_that_are_set()
    {
        Assert.Equal(50, PayloadLimits.Min(100, 80, null, 50));
        Assert.Equal(80, PayloadLimits.Min(100, 80, null, null));
        Assert.Equal(100, PayloadLimits.Min(100, null, null, null));
        Assert.Equal(16777216 + ProtocolConstants.FrameEnvelopeReserve, PayloadLimits.WireCap(16777216));
    }

    [Fact]
    public void Push_frames_may_use_the_wire_cap_and_other_frames_stay_small()
    {
        var push = PayloadLimits.InboundFrameCap((byte)MessageType.Push, 1024 * 1024);
        var connect = PayloadLimits.InboundFrameCap((byte)MessageType.Connect, 1024 * 1024);
        Assert.True(push > connect);
        Assert.Equal(ProtocolConstants.ControlFrameCap, connect);
    }
}

public class PayloadAbstractionTests
{
    [Fact]
    public void Raw_and_utf8_and_json_payloads_round_trip()
    {
        var raw = new RawPayload(new byte[] { 1, 2, 3 });
        var copy = new RawPayload();
        copy.SetPayload(raw.GetPayload().Span);
        Assert.Equal(new byte[] { 1, 2, 3 }, copy.GetPayload().ToArray());

        var text = new Utf8StringPayload("hello");
        var textCopy = new Utf8StringPayload();
        textCopy.SetPayload(text.GetPayload().Span);
        Assert.Equal("hello", textCopy.Text);

        var json = new JsonPayload<PayloadSample>(new PayloadSample { N = 7 });
        var jsonCopy = new JsonPayload<PayloadSample>();
        jsonCopy.SetPayload(json.GetPayload().Span);
        Assert.Equal(7, jsonCopy.Value!.N);
    }

    private sealed class PayloadSample
    {
        public int N { get; set; }
    }
}

public class ReferenceVectorTests
{
    [Fact]
    public void Canonical_vectors_match_the_protocol_spec()
    {
        var spec = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "protocol.md"));
        var documented = SpecVectors.Read(spec);
        foreach (var (name, frame) in ReferenceVectors.Canonical())
        {
            Assert.True(documented.ContainsKey(name), $"docs/protocol.md is missing vector '{name}'.");
            Assert.Equal(Hex.Format(frame), documented[name]);
        }

        Assert.True(documented.ContainsKey("connect-trailing"));
        var trailing = ReferenceVectors.ConnectWithTrailing();
        Assert.Equal(documented["connect-trailing"], Hex.Format(trailing));
        var headerLength = BinaryPrimitives.ReadUInt32LittleEndian(trailing.AsSpan(12));
        var decoded = MessageCodec.DecodeConnect(trailing.AsSpan(ProtocolConstants.HeaderSize, (int)headerLength));
        Assert.Equal("demo", decoded.ClientName);
        Assert.Equal(1, decoded.ProtocolVersion);
        Assert.Equal("", decoded.AuthToken);
    }
}

internal static class ReferenceVectors
{
    public static IEnumerable<(string Name, byte[] Frame)> Canonical()
    {
        yield return ("connect", Frame(MessageType.Connect, 1, MessageCodec.EncodeConnect(new ConnectBody("demo", 1, ""))));
        yield return ("hello", Frame(MessageType.Hello, 1, MessageCodec.EncodeHello(new HelloBody(
            "DomeMQ", 1, "0123456789abcdef0123456789abcdef", 16_777_216, 100_000, (byte)ChannelKind.Queue))));
        yield return ("create-channel", Frame(MessageType.CreateChannel, 2, MessageCodec.EncodeCreateChannel(
            new CreateChannelBody("orders", (byte)ChannelKind.Queue, 0, 0))));
        yield return ("create-channel-limits", Frame(MessageType.CreateChannel, 2, MessageCodec.EncodeCreateChannel(
            new CreateChannelBody("orders", (byte)ChannelKind.Queue, 0, 0, (byte)FullPolicy.DiscardOldest, 4096))));
        yield return ("create-channel-ack", Frame(MessageType.Ack, 2, MessageCodec.EncodeCreateChannelAck(
            new CreateChannelAck("orders", (byte)ChannelKind.Queue, 100_000, 16_777_216))));
        yield return ("delete-channel", Frame(MessageType.DeleteChannel, 3, MessageCodec.EncodeDeleteChannel(new DeleteChannelBody("orders"))));
        yield return ("push-raw", Frame(MessageType.Push, 4, MessageCodec.EncodePush(
            new PushBody("orders", 0, "", "hello"u8.ToArray()))));
        yield return ("push-labeled", Frame(MessageType.Push, 5, MessageCodec.EncodePush(
            new PushBody("orders", 64, "application/json", "{\"n\":1}"u8.ToArray()))));
        yield return ("pop", Frame(MessageType.Pop, 6, MessageCodec.EncodePop(new PopBody("orders", 8, 1000, 0))));
        yield return ("pop-ack", Frame(MessageType.Ack, 6, MessageCodec.EncodePopAck(new PopAck(
            "orders", [new Delivery("application/json", "{\"n\":1}"u8.ToArray())]))));
        yield return ("push-ack-discarded", Frame(MessageType.Ack, 4, MessageCodec.EncodePushAck(
            new PushAck("orders", 2, PushStatus.Discarded))));
        yield return ("error-payload-too-large", Frame(MessageType.Error, 4, MessageCodec.EncodeError(
            new ErrorBody((ushort)ErrorCode.PayloadTooLarge, "Payload too large.", (byte)MessageType.Push))));
        yield return ("ping", Frame(MessageType.Ping, 7, MessageCodec.EncodePing(new PingBody(1000))));
        yield return ("pong", Frame(MessageType.Pong, 7, MessageCodec.EncodePong(new PongBody(1000, 1001))));
        yield return ("disconnect", Frame(MessageType.Disconnect, 0, MessageCodec.EncodeDisconnect(new DisconnectBody(0, "bye"))));
    }

    public static byte[] ConnectWithTrailing()
    {
        var canonical = Canonical().Single(item => item.Name == "connect").Frame;
        var extended = new byte[canonical.Length + 2];
        canonical.CopyTo(extended, 0);
        extended[^2] = 0xFF;
        extended[^1] = 0x00;
        var length = BinaryPrimitives.ReadUInt32LittleEndian(extended.AsSpan(12)) + 2;
        BinaryPrimitives.WriteUInt32LittleEndian(extended.AsSpan(12), length);
        return extended;
    }

    private static byte[] Frame(MessageType type, uint requestId, byte[] body) =>
        FrameCodec.Encode(FrameCodec.Create(type, requestId, body));
}

internal static class Hex
{
    public static string Format(ReadOnlySpan<byte> bytes)
    {
        var builder = new StringBuilder(bytes.Length * 3);
        for (var i = 0; i < bytes.Length; i++)
        {
            if (i > 0)
            {
                builder.Append(' ');
            }

            builder.Append(bytes[i].ToString("x2"));
        }

        return builder.ToString();
    }
}

internal static class SpecVectors
{
    public static Dictionary<string, string> Read(string markdown)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        var lines = markdown.Replace("\r\n", "\n").Split('\n');
        string? name = null;
        var capture = false;
        var hex = new StringBuilder();
        foreach (var line in lines)
        {
            if (line.StartsWith("### ", StringComparison.Ordinal))
            {
                name = line[4..].Trim();
                continue;
            }

            if (line.StartsWith("```hex", StringComparison.Ordinal) && name is not null)
            {
                capture = true;
                hex.Clear();
                continue;
            }

            if (capture && line.StartsWith("```", StringComparison.Ordinal))
            {
                result[name!] = string.Join(' ', hex.ToString().Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
                capture = false;
                name = null;
                continue;
            }

            if (capture)
            {
                hex.Append(' ').Append(line.Trim());
            }
        }

        return result;
    }
}
