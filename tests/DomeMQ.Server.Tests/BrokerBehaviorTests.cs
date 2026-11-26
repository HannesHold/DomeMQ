using DomeMQ.Client;
using DomeMQ.Server;

namespace DomeMQ.Server.Tests;

public class BrokerBehaviorTests
{
    [Fact]
    public async Task Queue_is_fifo_stack_is_lifo_and_bag_is_complete()
    {
        var engine = Engine();
        var owner = Session(engine);
        await Connect(engine, owner);

        await Create(engine, owner, "q", ChannelKind.Queue);
        await Push(engine, owner, "q", "a"u8.ToArray());
        await Push(engine, owner, "q", "b"u8.ToArray());
        var queued = await Pop(engine, owner, "q", 2, 0);
        Assert.Equal(new[] { "a", "b" }, queued.Select(item => item.Body[0]).Select(value => ((char)value).ToString()).ToArray());

        await Create(engine, owner, "s", ChannelKind.Stack);
        await Push(engine, owner, "s", "a"u8.ToArray());
        await Push(engine, owner, "s", "b"u8.ToArray());
        var stacked = await Pop(engine, owner, "s", 2, 0);
        Assert.Equal(new[] { "b", "a" }, stacked.Select(item => ((char)item.Body[0]).ToString()).ToArray());

        await Create(engine, owner, "g", ChannelKind.Bag);
        await Push(engine, owner, "g", "a"u8.ToArray());
        await Push(engine, owner, "g", "b"u8.ToArray());
        await Push(engine, owner, "g", "c"u8.ToArray());
        var bag = await Pop(engine, owner, "g", 3, 0);
        Assert.Equal(new[] { "a", "b", "c" }, bag.Select(item => ((char)item.Body[0]).ToString()).OrderBy(item => item).ToArray());
    }

    [Fact]
    public async Task Only_the_owner_can_pop_or_delete_and_anyone_can_push()
    {
        var engine = Engine();
        var owner = Session(engine);
        var other = Session(engine);
        await Connect(engine, owner);
        await Connect(engine, other);
        await Create(engine, owner, "inbox", ChannelKind.Queue);

        var push = await Dispatch(engine, other, MessageType.Push, MessageCodec.EncodePush(new PushBody("inbox", 0, "text/plain", "hi"u8.ToArray())));
        Assert.Equal((byte)MessageType.Ack, push.Type);

        var stolen = await Dispatch(engine, other, MessageType.Pop, MessageCodec.EncodePop(new PopBody("inbox", 1, 0, 0)));
        Assert.Equal(ErrorCode.NotChannelOwner, (ErrorCode)Error(stolen).Code);

        var deleted = await Dispatch(engine, other, MessageType.DeleteChannel, MessageCodec.EncodeDeleteChannel(new DeleteChannelBody("inbox")));
        Assert.Equal(ErrorCode.NotChannelOwner, (ErrorCode)Error(deleted).Code);

        var messages = await Pop(engine, owner, "inbox", 1, 0);
        Assert.Equal("hi", Encoding.UTF8.GetString(messages[0].Body));
        Assert.Equal("text/plain", messages[0].ContentType);
    }

    [Fact]
    public async Task Full_channel_rejects_or_drops_the_oldest()
    {
        var rejecting = Engine(queueLength: 2, dropOldest: false);
        var owner = Session(rejecting);
        await Connect(rejecting, owner);
        await Create(rejecting, owner, "q", ChannelKind.Queue);
        await Push(rejecting, owner, "q", "a"u8.ToArray());
        await Push(rejecting, owner, "q", "b"u8.ToArray());
        var full = await Dispatch(rejecting, owner, MessageType.Push, MessageCodec.EncodePush(new PushBody("q", 0, "", "c"u8.ToArray())));
        Assert.Equal(ErrorCode.ChannelFull, (ErrorCode)Error(full).Code);
        var kept = await Pop(rejecting, owner, "q", 2, 0);
        Assert.Equal("ab", string.Concat(kept.Select(item => (char)item.Body[0])));

        var dropping = Engine(queueLength: 2, dropOldest: true);
        owner = Session(dropping);
        await Connect(dropping, owner);
        await Create(dropping, owner, "q", ChannelKind.Queue);
        await Push(dropping, owner, "q", "a"u8.ToArray());
        await Push(dropping, owner, "q", "b"u8.ToArray());
        await Push(dropping, owner, "q", "c"u8.ToArray());
        kept = await Pop(dropping, owner, "q", 2, 0);
        Assert.Equal("bc", string.Concat(kept.Select(item => (char)item.Body[0])));
    }

    [Fact]
    public async Task Payload_limits_use_the_tightest_level_and_channel_limits_above_global_are_rejected()
    {
        var engine = Engine(payload: 100);
        var session = Session(engine, connectionLimit: 40);
        await Connect(engine, session);

        var tooHigh = await Dispatch(engine, session, MessageType.CreateChannel, MessageCodec.EncodeCreateChannel(
            new CreateChannelBody("wide", (byte)ChannelKind.Queue, 0, 101)));
        Assert.Equal(ErrorCode.InvalidPayloadLimit, (ErrorCode)Error(tooHigh).Code);

        var created = await Dispatch(engine, session, MessageType.CreateChannel, MessageCodec.EncodeCreateChannel(
            new CreateChannelBody("box", (byte)ChannelKind.Queue, 0, 30)));
        Assert.Equal((byte)MessageType.Ack, created.Type);
        Assert.Equal(30u, MessageCodec.DecodeCreateChannelAck(created.Payload.Span).MaxPayloadSize);

        var overChannel = await Dispatch(engine, session, MessageType.Push, MessageCodec.EncodePush(
            new PushBody("box", 0, "", new byte[31])));
        Assert.Equal(ErrorCode.PayloadTooLarge, (ErrorCode)Error(overChannel).Code);

        var overMessage = await Dispatch(engine, session, MessageType.Push, MessageCodec.EncodePush(
            new PushBody("box", 10, "", new byte[11])));
        Assert.Equal(ErrorCode.PayloadTooLarge, (ErrorCode)Error(overMessage).Code);

        var accepted = await Dispatch(engine, session, MessageType.Push, MessageCodec.EncodePush(
            new PushBody("box", 10, "", new byte[10])));
        Assert.Equal((byte)MessageType.Ack, accepted.Type);

        var blocked = await Dispatch(engine, session, MessageType.Pop, MessageCodec.EncodePop(new PopBody("box", 1, 0, 4)));
        Assert.Equal(ErrorCode.PayloadTooLarge, (ErrorCode)Error(blocked).Code);
        var popped = await Pop(engine, session, "box", 1, 0);
        Assert.Equal(10, popped[0].Body.Length);
    }

    [Fact]
    public async Task Listener_limit_is_enforced_on_push_even_when_the_channel_cap_is_higher()
    {
        var engine = Engine(payload: 1000);
        var creator = Session(engine, connectionLimit: 1000);
        var publisher = Session(engine, connectionLimit: 20);
        await Connect(engine, creator);
        await Connect(engine, publisher);
        await Create(engine, creator, "box", ChannelKind.Queue);
        var rejected = await Dispatch(engine, publisher, MessageType.Push, MessageCodec.EncodePush(
            new PushBody("box", 0, "", new byte[21])));
        Assert.Equal(ErrorCode.PayloadTooLarge, (ErrorCode)Error(rejected).Code);
    }

    [Fact]
    public async Task Commands_before_connect_are_rejected_and_shared_keys_are_checked()
    {
        var engine = Engine();
        var session = Session(engine);
        var early = await Dispatch(engine, session, MessageType.Ping, MessageCodec.EncodePing(new PingBody(1)));
        Assert.Equal(ErrorCode.NotConnected, (ErrorCode)Error(early).Code);
        Assert.False(session.HandshakeComplete);

        var options = TestOptions();
        options.Authentication.Mode = "SharedKey";
        options.Authentication.Tokens = ["secret"];
        var authed = new BrokerEngine(options);
        var bad = Session(authed);
        var denied = await Dispatch(authed, bad, MessageType.Connect, MessageCodec.EncodeConnect(new ConnectBody("c", 1, "nope")));
        Assert.Equal(ErrorCode.Unauthorized, (ErrorCode)Error(denied).Code);

        var good = Session(authed);
        var hello = await Dispatch(authed, good, MessageType.Connect, MessageCodec.EncodeConnect(new ConnectBody("c", 1, "secret")));
        Assert.Equal((byte)MessageType.Hello, hello.Type);
    }

    [Fact]
    public async Task Ping_pong_and_unknown_flags()
    {
        var engine = Engine();
        var session = Session(engine);
        await Connect(engine, session);
        var pong = await Dispatch(engine, session, MessageType.Ping, MessageCodec.EncodePing(new PingBody(42)));
        Assert.Equal((byte)MessageType.Pong, pong.Type);
        Assert.Equal(42, MessageCodec.DecodePong(pong.Payload.Span).EchoedUnixTimeMilliseconds);

        var flagged = FrameCodec.Create(MessageType.Ping, 9, MessageCodec.EncodePing(new PingBody(1)), flags: 0x0002);
        var result = await engine.DispatchAsync(session, flagged, CancellationToken.None);
        Assert.Equal(ErrorCode.ProtocolError, (ErrorCode)Error(result.Response!.Value).Code);
    }

    [Fact]
    public async Task Messages_expire_and_orphaned_channels_are_removed()
    {
        var now = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var options = TestOptions();
        options.Channels.DefaultMessageTtl = TimeSpan.FromMinutes(1);
        options.Channels.OrphanChannelTtl = TimeSpan.FromMinutes(5);
        var engine = new BrokerEngine(options) { Clock = () => now };
        var owner = Session(engine);
        await Connect(engine, owner);
        await Create(engine, owner, "q", ChannelKind.Queue);
        await Push(engine, owner, "q", "a"u8.ToArray());
        now = now.AddMinutes(2);
        var expired = await Pop(engine, owner, "q", 1, 0);
        Assert.Empty(expired);

        await Push(engine, owner, "q", "b"u8.ToArray());
        engine.DropSession(owner);
        var other = Session(engine);
        await Connect(engine, other);
        var stillThere = await Dispatch(engine, other, MessageType.Push, MessageCodec.EncodePush(new PushBody("q", 0, "", "c"u8.ToArray())));
        Assert.Equal((byte)MessageType.Ack, stillThere.Type);
        engine.Cleanup(now.AddMinutes(6));
        var gone = await Dispatch(engine, other, MessageType.Push, MessageCodec.EncodePush(new PushBody("q", 0, "", "d"u8.ToArray())));
        Assert.Equal(ErrorCode.ChannelNotFound, (ErrorCode)Error(gone).Code);
    }

    [Fact]
    public async Task Pop_waits_until_a_message_arrives()
    {
        var engine = Engine();
        var owner = Session(engine);
        await Connect(engine, owner);
        await Create(engine, owner, "q", ChannelKind.Queue);
        var popping = Pop(engine, owner, "q", 1, 2000);
        await Push(engine, owner, "q", "z"u8.ToArray());
        var messages = await popping;
        Assert.Equal("z", Encoding.UTF8.GetString(messages[0].Body));
    }

    private static DomeMqOptions TestOptions() => new()
    {
        Limits =
        {
            MaxPayloadSize = 64 * 1024,
            MaxQueueLengthPerChannel = 100,
            MaxChannels = 100,
            MaxChannelsPerClient = 100,
            MaxClients = 100
        },
        Channels =
        {
            DefaultType = "Queue",
            OrphanChannelTtl = TimeSpan.Zero,
            CleanupInterval = TimeSpan.Zero,
            DefaultMessageTtl = null
        },
        Timeouts =
        {
            HeartbeatInterval = TimeSpan.Zero,
            IdleTimeout = TimeSpan.FromMinutes(5),
            HandshakeTimeout = TimeSpan.FromSeconds(5)
        }
    };

    private static BrokerEngine Engine(int payload = 64 * 1024, int queueLength = 100, bool dropOldest = false)
    {
        var options = TestOptions();
        options.Limits.MaxPayloadSize = payload;
        options.Limits.MaxQueueLengthPerChannel = queueLength;
        options.Limits.FullBehavior = dropOldest ? "DropOldest" : "Reject";
        return new BrokerEngine(options);
    }

    private static Session Session(BrokerEngine engine, int? connectionLimit = null) =>
        new()
        {
            ConnectionPayloadLimit = connectionLimit ?? 64 * 1024
        };

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

    private static async Task Push(BrokerEngine engine, Session session, string name, byte[] body)
    {
        var frame = await Dispatch(engine, session, MessageType.Push, MessageCodec.EncodePush(new PushBody(name, 0, "", body)));
        Assert.Equal((byte)MessageType.Ack, frame.Type);
    }

    private static async Task<Delivery[]> Pop(BrokerEngine engine, Session session, string name, int max, uint timeoutMs)
    {
        var frame = await Dispatch(engine, session, MessageType.Pop, MessageCodec.EncodePop(new PopBody(name, (uint)max, timeoutMs, 0)));
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
        var body = MessageCodec.DecodeError(frame.Payload.Span);
        return new ErrorBody(body.Code, body.Message, body.RelatedType);
    }
}

public class TransportAndAccessTests
{
    [Theory]
    [InlineData("tcp", true)]
    [InlineData("udp", true)]
    [InlineData("unix", false)]
    [InlineData("UnixSocket", false)]
    [InlineData("shm", false)]
    [InlineData("kcp", false)]
    public void Ip_rules_apply_only_to_ip_transports(string transport, bool applies)
    {
        Assert.Equal(applies, TransportRegistry.AppliesIpAccess(transport));
    }

    [Fact]
    public void Cidr_matching_allow_loopback_and_blocklist()
    {
        Assert.True(IpAccessControl.Contains(IPAddress.Parse("192.168.1.1"), IPAddress.Parse("192.168.1.0"), 24));
        Assert.True(IpAccessControl.Contains(IPAddress.Parse("192.168.1.255"), IPAddress.Parse("192.168.1.0"), 24));
        Assert.True(IpAccessControl.Contains(IPAddress.Parse("192.168.1.0"), IPAddress.Parse("192.168.1.0"), 24));
        Assert.False(IpAccessControl.Contains(IPAddress.Parse("192.168.2.1"), IPAddress.Parse("192.168.1.0"), 24));
        Assert.True(IpAccessControl.Contains(IPAddress.Parse("10.1.2.3"), IPAddress.Parse("0.0.0.0"), 0));
        Assert.True(IpAccessControl.Contains(IPAddress.Parse("2001:db8::1"), IPAddress.Parse("2001:db8::"), 32));
        Assert.False(IpAccessControl.Contains(IPAddress.Parse("2001:db9::1"), IPAddress.Parse("2001:db8::"), 32));
        Assert.True(IpAccessControl.Contains(IPAddress.Parse("::ffff:192.168.1.9"), IPAddress.Parse("192.168.1.0"), 24));

        var allow = IpAccessControl.Create(new IpAccessOptions
        {
            Enabled = true,
            Mode = "Whitelist",
            AllowLoopback = true,
            Entries = []
        });
        Assert.True(allow.IsAllowed(IPAddress.Loopback));
        Assert.True(allow.IsAllowed(IPAddress.IPv6Loopback));
        Assert.False(allow.IsAllowed(IPAddress.Parse("8.8.8.8")));

        var noLoop = IpAccessControl.Create(new IpAccessOptions
        {
            Enabled = true,
            Mode = "Allowlist",
            AllowLoopback = false,
            Entries = []
        });
        Assert.False(noLoop.IsAllowed(IPAddress.Parse("127.0.0.1")));

        var block = IpAccessControl.Create(new IpAccessOptions
        {
            Enabled = true,
            Mode = "Blocklist",
            AllowLoopback = true,
            Entries = ["10.0.0.0/8", "127.0.0.1"]
        });
        Assert.False(block.IsAllowed(IPAddress.Parse("10.1.2.3")));
        Assert.True(block.IsAllowed(IPAddress.Parse("192.168.0.1")));
        Assert.True(block.IsAllowed(IPAddress.Parse("127.0.0.1")));

        var blockLoop = IpAccessControl.Create(new IpAccessOptions
        {
            Enabled = true,
            Mode = "Blocklist",
            AllowLoopback = false,
            Entries = ["127.0.0.1/32"]
        });
        Assert.False(blockLoop.IsAllowed(IPAddress.Parse("127.0.0.1")));

        var disabled = IpAccessControl.Create(new IpAccessOptions
        {
            Enabled = false,
            Mode = "Allowlist",
            Entries = []
        });
        Assert.True(disabled.IsAllowed(IPAddress.Parse("8.8.8.8")));

        Assert.Throws<FormatException>(() => IpAccessControl.Parse("not-an-ip"));
        Assert.Throws<FormatException>(() => IpAccessControl.Parse("192.168.1.1/33"));
        Assert.Throws<FormatException>(() => IpAccessControl.Parse("::1/129"));
    }

    [Fact]
    public void Per_listener_override_replaces_the_global_list_and_can_be_reloaded()
    {
        var options = new DomeMqOptions
        {
            IpAccess = new IpAccessOptions
            {
                Enabled = true,
                Mode = "Allowlist",
                AllowLoopback = false,
                Entries = ["10.0.0.0/8"]
            },
            Listeners =
            [
                new ListenerOptions { Name = "tcp", Transport = "Tcp", Host = "127.0.0.1", Port = 0 },
                new ListenerOptions
                {
                    Name = "other",
                    Transport = "Tcp",
                    Host = "127.0.0.1",
                    Port = 0,
                    IpAccess = new IpAccessOptions
                    {
                        Enabled = true,
                        Mode = "Blocklist",
                        AllowLoopback = true,
                        Entries = ["192.168.0.0/16"]
                    }
                }
            ]
        };

        var broker = new DomeMqBroker(options);
        broker.UpdateIpAccess(options);
        Assert.False(broker.IsIpAllowed("tcp", IPAddress.Parse("127.0.0.1")));
        Assert.True(broker.IsIpAllowed("tcp", IPAddress.Parse("10.1.2.3")));
        Assert.False(broker.IsIpAllowed("other", IPAddress.Parse("192.168.1.5")));
        Assert.True(broker.IsIpAllowed("other", IPAddress.Loopback));

        options.IpAccess.Entries.Add("127.0.0.1");
        broker.UpdateIpAccess(options);
        Assert.True(broker.IsIpAllowed("tcp", IPAddress.Parse("127.0.0.1")));
    }

    [Fact]
    public void Stubs_are_registered_and_not_implemented()
    {
        foreach (var name in new[] { "udp", "kcp", "shm" })
        {
            var factory = TransportRegistry.Resolve(name);
            Assert.False(factory.IsImplemented);
            Assert.Throws<TransportNotSupportedException>(() => factory.Listen(new TransportSettings()));
        }

        Assert.True(TransportRegistry.Resolve("tcp").IsImplemented);
        Assert.True(TransportRegistry.Resolve("unixsocket").IsImplemented);
    }

    [Fact]
    public void Invalid_configuration_is_reported()
    {
        var options = new DomeMqOptions
        {
            Limits = { MaxPayloadSize = 0, MaxChannelsPerClient = 5, MaxChannels = 1 },
            Authentication = { Mode = "SharedKey", Tokens = [] },
            IpAccess = { Enabled = true, Entries = ["nope"] },
            Listeners = [new ListenerOptions { Name = "tcp", Transport = "Udp", Enabled = true, Host = "127.0.0.1", Port = 1 }]
        };
        var errors = DomeMqOptionsValidator.Validate(options);
        Assert.Contains(errors, error => error.Contains("MaxPayloadSize", StringComparison.Ordinal));
        Assert.Contains(errors, error => error.Contains("SharedKey", StringComparison.Ordinal));
        Assert.Contains(errors, error => error.Contains("nope", StringComparison.Ordinal));
        Assert.Contains(errors, error => error.Contains("not implemented", StringComparison.Ordinal));
    }
}

public class EndToEndTests
{
    [Fact]
    public async Task Tcp_client_exchanges_raw_json_and_custom_payloads()
    {
        var options = ListeningOptions();
        await using var broker = new DomeMqBroker(options);
        await broker.StartAsync();
        var endpoint = Assert.IsType<IPEndPoint>(broker.Listeners[0].LocalEndPoint);

        await using var client = await DomeMqClient.ConnectAsync(new DomeMqClientOptions
        {
            Host = "127.0.0.1",
            Port = endpoint.Port,
            HeartbeatInterval = TimeSpan.Zero,
            MaxPayloadSize = 32
        });

        var info = await client.CreateChannelAsync("orders", ChannelKind.Queue);
        Assert.Equal(ChannelKind.Queue, info.Kind);
        await client.PushAsync("orders", "hello"u8.ToArray());
        await client.PushAsync("orders", new Utf8StringPayload("world"));
        await client.PushAsync("orders", new JsonPayload<OrderNote>(new OrderNote { N = 7 }), new PushOptions { ContentType = "application/json" });
        await client.PushAsync("orders", 42, new Int32BeSerializer());

        var messages = await client.PopAsync("orders", new PopOptions { MaxMessages = 10, Timeout = TimeSpan.Zero });
        Assert.Equal(4, messages.Count);
        Assert.Equal("hello", Encoding.UTF8.GetString(messages[0].Body));
        Assert.Equal("world", Encoding.UTF8.GetString(messages[1].Body));
        Assert.Equal("application/json", messages[2].ContentType);
        Assert.Equal(7, JsonSerializer.Deserialize<OrderNote>(messages[2].Body)!.N);
        Assert.Equal(42, new Int32BeSerializer().Deserialize(messages[3].Body));

        var text = await client.PopAsync<Utf8StringPayload>("orders", new PopOptions { Timeout = TimeSpan.FromMilliseconds(50) });
        Assert.Empty(text);

        await client.PushAsync("orders", new byte[32]);
        await Assert.ThrowsAsync<PayloadTooLargeException>(() => client.PushAsync("orders", new byte[33]));
        await client.DeleteChannelAsync("orders");
        await client.DisconnectAsync();
    }

    [Fact]
    public async Task Unix_socket_round_trip()
    {
        try
        {
            using var probe = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            probe.Dispose();
        }
        catch (SocketException)
        {
            return;
        }

        var path = Path.Combine(Path.GetTempPath(), $"d{Guid.NewGuid():N}.sock");
        var options = ListeningOptions();
        options.Listeners.Clear();
        options.Listeners.Add(new ListenerOptions
        {
            Name = "unix",
            Transport = "UnixSocket",
            Enabled = true,
            Path = path
        });
        await using var broker = new DomeMqBroker(options);
        await broker.StartAsync();
        try
        {
            await using var client = await DomeMqClient.ConnectAsync(new DomeMqClientOptions
            {
                Transport = "unix",
                Path = path,
                HeartbeatInterval = TimeSpan.Zero
            });
            await client.CreateChannelAsync("q", ChannelKind.Queue);
            await client.PushAsync("q", "socket"u8.ToArray());
            var messages = await client.PopAsync("q", new PopOptions { Timeout = TimeSpan.Zero });
            Assert.Equal("socket", Encoding.UTF8.GetString(messages[0].Body));
        }
        finally
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }

    [Fact]
    public async Task Disallowed_ip_is_rejected_before_handshake()
    {
        var options = ListeningOptions();
        options.IpAccess = new IpAccessOptions
        {
            Enabled = true,
            Mode = "Allowlist",
            AllowLoopback = false,
            Entries = ["10.0.0.0/8"]
        };
        await using var broker = new DomeMqBroker(options);
        await broker.StartAsync();
        var endpoint = Assert.IsType<IPEndPoint>(broker.Listeners[0].LocalEndPoint);
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var client = await DomeMqClient.ConnectAsync(new DomeMqClientOptions
            {
                Host = "127.0.0.1",
                Port = endpoint.Port,
                HeartbeatInterval = TimeSpan.Zero,
                RequestTimeout = TimeSpan.FromSeconds(2)
            });
        });
    }

    [Fact]
    public async Task Health_endpoint_responds()
    {
        var options = ListeningOptions();
        options.Health.Enabled = true;
        options.Health.Host = "127.0.0.1";
        options.Health.Port = 0;
        options.Health.Path = "/health";
        await using var broker = new DomeMqBroker(options);
        await broker.StartAsync();
        var endpoint = Assert.IsType<IPEndPoint>(broker.HealthEndPoint);
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await socket.ConnectAsync(new IPEndPoint(IPAddress.Loopback, endpoint.Port));
        var request = "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n"u8.ToArray();
        await socket.SendAsync(request);
        var buffer = new byte[512];
        var read = await socket.ReceiveAsync(buffer);
        var text = Encoding.ASCII.GetString(buffer, 0, read);
        Assert.Contains("200 OK", text, StringComparison.Ordinal);
        Assert.Contains("ok", text, StringComparison.Ordinal);
    }

    private static DomeMqOptions ListeningOptions()
    {
        var options = DomeMqOptions.CreateDefault();
        options.Listeners[0].Host = "127.0.0.1";
        options.Listeners[0].Port = 0;
        options.Timeouts.HeartbeatInterval = TimeSpan.Zero;
        options.Timeouts.IdleTimeout = TimeSpan.FromMinutes(1);
        options.Channels.OrphanChannelTtl = TimeSpan.Zero;
        options.Channels.CleanupInterval = TimeSpan.Zero;
        return options;
    }

    private sealed class OrderNote
    {
        public int N { get; set; }
    }

    private sealed class Int32BeSerializer : IPayloadSerializer<int>
    {
        public void Serialize(int value, IBufferWriter<byte> destination)
        {
            var span = destination.GetSpan(4);
            BinaryPrimitives.WriteInt32BigEndian(span, value);
            destination.Advance(4);
        }

        public int Deserialize(ReadOnlySpan<byte> source) => BinaryPrimitives.ReadInt32BigEndian(source);
    }
}
