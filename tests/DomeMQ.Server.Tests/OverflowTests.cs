namespace DomeMQ.Server.Tests;

public class OverflowTests
{
    public static IEnumerable<object[]> Policies()
    {
        foreach (var kind in new[] { ChannelKind.Queue, ChannelKind.Stack, ChannelKind.Bag })
        {
            yield return [kind, FullPolicy.Reject];
            yield return [kind, FullPolicy.DiscardNewest];
            yield return [kind, FullPolicy.DiscardOldest];
            yield return [kind, FullPolicy.ClearAll];
        }
    }

    [Theory]
    [MemberData(nameof(Policies))]
    public async Task Every_policy_applies_to_every_channel_type(ChannelKind kind, FullPolicy policy)
    {
        var engine = Engine(policy);
        var session = Session();
        await Connect(engine, session);
        await Create(engine, session, "box", kind);
        await Push(engine, session, "box", "a"u8.ToArray(), expectDiscarded: false);
        await Push(engine, session, "box", "b"u8.ToArray(), expectDiscarded: false);
        var third = await Dispatch(engine, session, MessageType.Push, MessageCodec.EncodePush(new PushBody("box", 0, "", "c"u8.ToArray())));
        var stored = await Pop(engine, session, "box", 10);

        if (policy == FullPolicy.Reject)
        {
            Assert.Equal(ErrorCode.ChannelFull, (ErrorCode)Error(third).Code);
            Assert.Equal(["a", "b"], Names(stored));
            return;
        }

        var ack = MessageCodec.DecodePushAck(third.Payload.Span);
        if (policy == FullPolicy.DiscardNewest)
        {
            Assert.Equal(PushStatus.Discarded, ack.Status);
            Assert.Equal(2u, ack.Depth);
            Assert.Equal(["a", "b"], Names(stored));
            return;
        }

        Assert.Equal(PushStatus.Accepted, ack.Status);
        Assert.Equal(policy == FullPolicy.ClearAll ? ["c"] : ["b", "c"], Names(stored));
    }

    [Fact]
    public async Task Global_memory_cap_rejects_or_discards_across_channels()
    {
        var rejecting = Engine(FullPolicy.Reject, maxMessages: 10, maxQueuedBytes: 5);
        var session = Session();
        await Connect(rejecting, session);
        await Create(rejecting, session, "a", ChannelKind.Queue);
        await Create(rejecting, session, "b", ChannelKind.Queue);
        await Push(rejecting, session, "a", [1, 2, 3], expectDiscarded: false);
        var blocked = await Dispatch(rejecting, session, MessageType.Push, MessageCodec.EncodePush(new PushBody("b", 0, "", [4, 5, 6])));
        Assert.Equal(ErrorCode.ServerMemoryCap, (ErrorCode)Error(blocked).Code);

        var dropping = Engine(FullPolicy.DiscardOldest, maxMessages: 10, maxQueuedBytes: 5);
        session = Session();
        await Connect(dropping, session);
        await Create(dropping, session, "a", ChannelKind.Queue);
        await Push(dropping, session, "a", [1, 2, 3], expectDiscarded: false);
        await Push(dropping, session, "a", [4, 5, 6], expectDiscarded: false);
        var left = await Pop(dropping, session, "a", 10);
        var only = Assert.Single(left);
        Assert.Equal(new byte[] { 4, 5, 6 }, only.Body);
    }

    [Fact]
    public async Task Per_client_cap_counts_unconsumed_pushes()
    {
        var engine = Engine(FullPolicy.Reject, maxMessages: 100, perClient: 2);
        var session = Session();
        var other = Session();
        await Connect(engine, session);
        await Connect(engine, other);
        await Create(engine, session, "a", ChannelKind.Queue);
        await Create(engine, session, "b", ChannelKind.Queue);
        await Push(engine, session, "a", "a"u8.ToArray(), expectDiscarded: false);
        await Push(engine, session, "b", "b"u8.ToArray(), expectDiscarded: false);
        var blocked = await Dispatch(engine, session, MessageType.Push, MessageCodec.EncodePush(new PushBody("a", 0, "", "c"u8.ToArray())));
        Assert.Equal(ErrorCode.ClientQueueLimit, (ErrorCode)Error(blocked).Code);
        await Push(engine, other, "a", "d"u8.ToArray(), expectDiscarded: false);
        var stored = await Pop(engine, session, "a", 10);
        Assert.Equal(["a", "d"], Names(stored));
    }

    [Fact]
    public async Task Warning_fires_once_per_upward_crossing()
    {
        var engine = Engine(FullPolicy.Reject, maxMessages: 10, warning: 80);
        var seen = new List<QueuePressure>();
        engine.Pressure += notice =>
        {
            if (notice.Warning && notice.Scope == "box")
            {
                seen.Add(notice);
            }
        };
        var session = Session();
        await Connect(engine, session);
        await Create(engine, session, "box", ChannelKind.Queue);
        for (var i = 0; i < 7; i++)
        {
            await Push(engine, session, "box", [(byte)i], expectDiscarded: false);
        }

        Assert.Empty(seen);
        await Push(engine, session, "box", [7], expectDiscarded: false);
        Assert.Equal(80, seen[0].FillPercent);
        await Push(engine, session, "box", [8], expectDiscarded: false);
        Assert.Single(seen);
        await Pop(engine, session, "box", 2);
        await Push(engine, session, "box", [9], expectDiscarded: false);
        Assert.Equal(2, seen.Count);
        var stats = engine.GetStatistics();
        Assert.Equal(8, stats.Channels.Single().QueuedMessages);
        Assert.True(stats.Channels.Single().WarningActive);
    }

    [Fact]
    public async Task Create_rejects_limits_above_the_global_values_and_uses_the_minimum()
    {
        var options = TestOptions(FullPolicy.Reject, 10);
        options.Limits.MaxBytesPerChannel = 100;
        options.Channels.Overrides.Add(new ChannelLimitOverride { Name = "box", MaxMessages = 4, MaxBytes = 50 });
        var engine = new BrokerEngine(options);
        var session = Session();
        await Connect(engine, session);
        var tooMany = await Dispatch(engine, session, MessageType.CreateChannel, MessageCodec.EncodeCreateChannel(
            new CreateChannelBody("wide", 1, 11, 0)));
        Assert.Equal(ErrorCode.InvalidQueueLimit, (ErrorCode)Error(tooMany).Code);
        var tooBig = await Dispatch(engine, session, MessageType.CreateChannel, MessageCodec.EncodeCreateChannel(
            new CreateChannelBody("wide", 1, 0, 0, 0, 200)));
        Assert.Equal(ErrorCode.InvalidByteLimit, (ErrorCode)Error(tooBig).Code);
        var created = await Dispatch(engine, session, MessageType.CreateChannel, MessageCodec.EncodeCreateChannel(
            new CreateChannelBody("box", 1, 6, 0, (byte)FullPolicy.DiscardNewest, 40)));
        var ack = MessageCodec.DecodeCreateChannelAck(created.Payload.Span);
        Assert.Equal(4u, ack.MaxQueueLength);
        Assert.Equal(40u, ack.MaxBytes);
        Assert.Equal((byte)FullPolicy.DiscardNewest, ack.FullPolicy);
    }

    [Fact]
    public void Short_create_messages_still_decode()
    {
        var body = MessageCodec.EncodeCreateChannel(new CreateChannelBody("orders", 1, 0, 0));
        var decoded = MessageCodec.DecodeCreateChannel(body);
        Assert.Equal((byte)0, decoded.FullPolicy);
        Assert.Equal(0u, decoded.MaxBytes);
        var ack = MessageCodec.DecodeCreateChannelAck(MessageCodec.EncodeCreateChannelAck(new CreateChannelAck("orders", 1, 4, 5)));
        Assert.Equal((byte)0, ack.FullPolicy);
        var push = MessageCodec.DecodePushAck(MessageCodec.EncodePushAck(new PushAck("orders", 2)));
        Assert.Equal(PushStatus.Accepted, push.Status);
    }

    private static string[] Names(Delivery[] messages) =>
        messages.Select(message => ((char)message.Body[0]).ToString()).OrderBy(static name => name).ToArray();

    private static DomeMqOptions TestOptions(FullPolicy policy, int maxMessages, long? maxBytes = null, long maxQueuedBytes = 1_000_000, int perClient = 1000, int warning = 80) => new()
    {
        Limits =
        {
            MaxMessagesPerChannel = maxMessages,
            MaxBytesPerChannel = maxBytes,
            MaxQueuedBytes = maxQueuedBytes,
            MaxMessagesPerClient = perClient,
            FullBehavior = policy.ToString(),
            WarningPercent = warning,
            MaxPayloadSize = 64 * 1024,
            MaxChannels = 100,
            MaxChannelsPerClient = 100,
            MaxClients = 100
        },
        Channels = { OrphanChannelTtl = TimeSpan.Zero, CleanupInterval = TimeSpan.Zero }
    };

    private static BrokerEngine Engine(FullPolicy policy, int maxMessages = 2, long? maxBytes = null, long maxQueuedBytes = 1_000_000, int perClient = 1000, int warning = 80) =>
        new(TestOptions(policy, maxMessages, maxBytes, maxQueuedBytes, perClient, warning));

    private static Session Session() => new() { ConnectionPayloadLimit = 64 * 1024 };

    private static async Task Connect(BrokerEngine engine, Session session)
    {
        var frame = await Dispatch(engine, session, MessageType.Connect, MessageCodec.EncodeConnect(new ConnectBody("test", 1, "")));
        Assert.Equal((byte)MessageType.Hello, frame.Type);
    }

    private static async Task Create(BrokerEngine engine, Session session, string name, ChannelKind kind)
    {
        var frame = await Dispatch(engine, session, MessageType.CreateChannel, MessageCodec.EncodeCreateChannel(
            new CreateChannelBody(name, (byte)kind, 0, 0)));
        Assert.Equal((byte)MessageType.Ack, frame.Type);
    }

    private static async Task Push(BrokerEngine engine, Session session, string name, byte[] body, bool expectDiscarded)
    {
        var frame = await Dispatch(engine, session, MessageType.Push, MessageCodec.EncodePush(new PushBody(name, 0, "", body)));
        Assert.Equal((byte)MessageType.Ack, frame.Type);
        Assert.Equal(expectDiscarded ? PushStatus.Discarded : PushStatus.Accepted, MessageCodec.DecodePushAck(frame.Payload.Span).Status);
    }

    private static async Task<Delivery[]> Pop(BrokerEngine engine, Session session, string name, int max)
    {
        var frame = await Dispatch(engine, session, MessageType.Pop, MessageCodec.EncodePop(new PopBody(name, (uint)max, 0, 0)));
        Assert.Equal((byte)MessageType.Ack, frame.Type);
        return MessageCodec.DecodePopAck(frame.Payload.Span).Messages;
    }

    private static async Task<Frame> Dispatch(BrokerEngine engine, Session session, MessageType type, byte[] body)
    {
        var result = await engine.DispatchAsync(session, FrameCodec.Create(type, 1, body), CancellationToken.None);
        Assert.True(result.Response.HasValue);
        return result.Response.Value;
    }

    private static ErrorBody Error(Frame frame)
    {
        Assert.Equal((byte)MessageType.Error, frame.Type);
        return MessageCodec.DecodeError(frame.Payload.Span);
    }
}
