using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Text;
using DomeMQ;
using DomeMQ.Client;
using DomeMQ.Server;

var unixPath = Path.Combine(Path.GetTempPath(), "dmq-" + Guid.NewGuid().ToString("N")[..8] + ".sock");
var options = new DomeMqOptions
{
    Timeouts = { HeartbeatInterval = TimeSpan.Zero },
    Channels = { OrphanChannelTtl = TimeSpan.Zero, CleanupInterval = TimeSpan.Zero }
};
options.Listeners.Add(new ListenerOptions
{
    Name = "tcp",
    Transport = "Tcp",
    Host = "127.0.0.1",
    Port = 0
});
options.Listeners.Add(new ListenerOptions
{
    Name = "ipc",
    Transport = "UnixSocket",
    Path = unixPath
});

try
{
    await using var broker = new DomeMqBroker(options);
    await broker.StartAsync();
    var port = TcpPort(broker);

    await using var owner = await DomeMqClient.ConnectAsync(TcpClient(port));
    var created = await owner.CreateChannelAsync("orders", ChannelKind.Queue);
    Console.WriteLine($"created {created.Name} {created.Kind} maxMessages={created.MaxQueueLength}");

    await owner.PushAsync("orders", "hello"u8.ToArray(), new PushOptions { ContentType = "text/plain" });
    var own = await owner.PopAsync("orders", new PopOptions { Timeout = TimeSpan.Zero });
    Expect(own[0].ContentType, "text/plain");
    Expect(Encoding.UTF8.GetString(own[0].Body), "hello");

    await using var peer = await DomeMqClient.ConnectAsync(new DomeMqClientOptions
    {
        Transport = "unix",
        Path = unixPath,
        ClientName = "unix-peer",
        HeartbeatInterval = TimeSpan.Zero
    });
    await peer.PushAsync("orders", "from-unix"u8.ToArray());
    var fromUnix = await owner.PopAsync("orders", new PopOptions { Timeout = TimeSpan.Zero });
    Expect(Encoding.UTF8.GetString(fromUnix[0].Body), "from-unix");

    await owner.PushAsync("orders", new Note("payload"));
    var notes = await owner.PopAsync<Note>("orders", new PopOptions { Timeout = TimeSpan.Zero });
    Expect(notes[0].Text, "payload");

    await owner.PushAsync("orders", 42, new Int32BeSerializer());
    var numbers = await owner.PopAsync("orders", new Int32BeSerializer(), new PopOptions { Timeout = TimeSpan.Zero });
    Expect(numbers[0], 42);

    Console.WriteLine("BasicQueue ok");
}
finally
{
    if (File.Exists(unixPath))
    {
        File.Delete(unixPath);
    }
}

static int TcpPort(DomeMqBroker broker)
{
    if (broker.Listeners[0].LocalEndPoint is not IPEndPoint endPoint)
    {
        throw new InvalidOperationException("The TCP listener did not bind an IP endpoint.");
    }

    return endPoint.Port;
}

static DomeMqClientOptions TcpClient(int port) => new()
{
    Transport = "tcp",
    Host = "127.0.0.1",
    Port = port,
    ClientName = "owner",
    HeartbeatInterval = TimeSpan.Zero,
    PopTimeout = TimeSpan.FromSeconds(2),
    RequestTimeout = TimeSpan.FromSeconds(5)
};

static void Expect<T>(T actual, T expected)
{
    if (!EqualityComparer<T>.Default.Equals(actual, expected))
    {
        throw new InvalidOperationException($"Expected {expected} but got {actual}.");
    }
}

sealed class Note : IPayload
{
    public Note()
    {
    }

    public Note(string text) => Text = text;

    public string Text { get; set; } = "";

    public int? SizeHint => Encoding.UTF8.GetByteCount(Text);

    public ReadOnlyMemory<byte> GetPayload() => Encoding.UTF8.GetBytes(Text);

    public void SetPayload(ReadOnlySpan<byte> data) => Text = Encoding.UTF8.GetString(data);
}

sealed class Int32BeSerializer : IPayloadSerializer<int>
{
    public void Serialize(int value, IBufferWriter<byte> destination)
    {
        var span = destination.GetSpan(4);
        BinaryPrimitives.WriteInt32BigEndian(span, value);
        destination.Advance(4);
    }

    public int Deserialize(ReadOnlySpan<byte> source) => BinaryPrimitives.ReadInt32BigEndian(source);
}
