using System.Net;
using System.Text;
using DomeMQ;
using DomeMQ.Client;
using DomeMQ.Server;

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

await using var broker = new DomeMqBroker(options);
await broker.StartAsync();
var port = TcpPort(broker);

await using var owner = await DomeMqClient.ConnectAsync(Client(port, "owner"));
await using var peer = await DomeMqClient.ConnectAsync(Client(port, "peer"));

var empty = await CreateAndPop(owner, "idle", ChannelKind.Queue, 1);
if (empty.Count != 0)
{
    throw new InvalidOperationException("An empty pop with a zero timeout should return nothing.");
}

await Push(owner, "fifo", ChannelKind.Queue, "a", "b", "c");
var queued = await owner.PopAsync("fifo", Immediate(2));
Expect(Texts(queued), "a", "b");
var queuedTail = await owner.PopAsync("fifo", Immediate(10));
Expect(Texts(queuedTail), "c");

await Push(owner, "lifo", ChannelKind.Stack, "a", "b", "c");
var stacked = await owner.PopAsync("lifo", Immediate(2));
Expect(Texts(stacked), "c", "b");
var stackedTail = await owner.PopAsync("lifo", Immediate(10));
Expect(Texts(stackedTail), "a");

await Push(owner, "parts", ChannelKind.Bag, "a", "b", "c");
var bag = await owner.PopAsync("parts", Immediate(3));
var observed = Texts(bag);
Console.WriteLine("bag observed order: " + string.Join(", ", observed));
Expect(observed.OrderBy(static item => item, StringComparer.Ordinal).ToArray(), "a", "b", "c");

await owner.CreateChannelAsync("partial", ChannelKind.Queue);
await owner.PushAsync("partial", Body("only"));
var one = await owner.PopAsync("partial", Immediate(5));
Expect(Texts(one), "only");

await owner.CreateChannelAsync("inbox", ChannelKind.Queue);
await peer.PushAsync("inbox", Body("from-peer"));
try
{
    await peer.PopAsync("inbox", Immediate(1));
    throw new InvalidOperationException("A non-owner pop should fail.");
}
catch (DomeMqException ex) when (ex.Code == ErrorCode.NotChannelOwner)
{
    Console.WriteLine($"peer pop rejected: {ex.Code}");
}

Expect(Texts(await owner.PopAsync("inbox", Immediate(1))), "from-peer");
var waiting = owner.PopAsync("inbox", new PopOptions { Timeout = TimeSpan.FromSeconds(3) });
await Task.Delay(200);
await peer.PushAsync("inbox", Body("woke"));
Expect(Texts(await waiting), "woke");

Console.WriteLine("ChannelKinds ok");

static async Task<IReadOnlyList<DomeMqMessage>> CreateAndPop(DomeMqClient owner, string name, ChannelKind kind, int maxMessages)
{
    await owner.CreateChannelAsync(name, kind);
    return await owner.PopAsync(name, Immediate(maxMessages));
}

static async Task Push(DomeMqClient owner, string name, ChannelKind kind, params string[] texts)
{
    await owner.CreateChannelAsync(name, kind);
    foreach (var text in texts)
    {
        await owner.PushAsync(name, Body(text));
    }
}

static PopOptions Immediate(int maxMessages) => new()
{
    MaxMessages = maxMessages,
    Timeout = TimeSpan.Zero
};

static byte[] Body(string text) => Encoding.UTF8.GetBytes(text);

static string[] Texts(IReadOnlyList<DomeMqMessage> messages) =>
    messages.Select(static message => Encoding.UTF8.GetString(message.Body)).ToArray();

static int TcpPort(DomeMqBroker broker)
{
    if (broker.Listeners[0].LocalEndPoint is not IPEndPoint endPoint)
    {
        throw new InvalidOperationException("The TCP listener did not bind an IP endpoint.");
    }

    return endPoint.Port;
}

static DomeMqClientOptions Client(int port, string name) => new()
{
    Transport = "tcp",
    Host = "127.0.0.1",
    Port = port,
    ClientName = name,
    HeartbeatInterval = TimeSpan.Zero,
    PopTimeout = TimeSpan.FromSeconds(2),
    RequestTimeout = TimeSpan.FromSeconds(5)
};

static void Expect(IReadOnlyList<string> actual, params string[] expected)
{
    if (actual.Count != expected.Length || !actual.SequenceEqual(expected))
    {
        throw new InvalidOperationException(
            $"Expected [{string.Join(", ", expected)}] but got [{string.Join(", ", actual)}].");
    }
}
