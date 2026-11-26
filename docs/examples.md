# Examples

These programs compile against the repository projects. They are the sources under `samples/`. None of them is packed as a NuGet package. Channel behavior is in [channels.md](channels.md). Broker settings are in [configuration.md](configuration.md). TLS files are in [keys.md](keys.md).

## Embedding the broker, TCP, and a Unix socket

The owner connects over TCP and creates a queue. A second connection pushes over a Unix socket. The same process then round-trips an `IPayload` and an `IPayloadSerializer<int>`. `dotnet run --project samples/BasicQueue`.

`samples/BasicQueue/Program.cs`:

```csharp
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
```

## Queue, stack, and bag

FIFO, LIFO, and an unordered bag, plus a batch that does not wait to fill, a zero timeout on an empty channel, a non-owner pop, and a pop that waits until another connection pushes. `dotnet run --project samples/ChannelKinds`.

`samples/ChannelKinds/Program.cs`:

```csharp
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
```

## Overflow, discarded pushes, and errors 16–19

Create limits are the minimum of the global cap, the named override, and the request. `DiscardNewest` returns `PushResult.Discarded`. `InvalidQueueLimit` is 16 and `InvalidByteLimit` is 19. `ClientQueueLimit` is 17 and `ServerMemoryCap` is 18. The last broker subscribes to `QueuePressure` and reads `GetStatistics()`. `dotnet run --project samples/OverflowPolicies`.

`samples/OverflowPolicies/Program.cs`:

```csharp
using System.Net;
using System.Text;
using DomeMQ;
using DomeMQ.Client;
using DomeMQ.Server;

await PoliciesAsync();
await ClientCapAsync();
await MemoryCapAsync();
await PressureAsync();
Console.WriteLine("OverflowPolicies ok");

static async Task PoliciesAsync()
{
    var options = BrokerOptions();
    options.Limits.MaxMessagesPerChannel = 8;
    options.Limits.MaxBytesPerChannel = 100;
    options.Limits.MaxQueuedBytes = 1_000_000;
    options.Limits.FullBehavior = "Reject";
    options.Channels.Overrides.Add(new ChannelLimitOverride
    {
        Name = "capped",
        MaxMessages = 2,
        MaxBytes = 20,
        FullBehavior = "DiscardNewest"
    });

    await using var broker = await StartAsync(options);
    await using var client = await ConnectAsync(broker);

    var capped = await client.CreateChannelAsync("capped", ChannelKind.Queue, new CreateChannelOptions
    {
        MaxQueueLength = 4,
        MaxBytes = 40
    });
    if (capped.MaxQueueLength != 2 || capped.MaxBytes != 20 || capped.FullPolicy != FullPolicy.DiscardNewest)
    {
        throw new InvalidOperationException(
            $"Effective cap was messages={capped.MaxQueueLength} bytes={capped.MaxBytes} policy={capped.FullPolicy}.");
    }

    await client.PushAsync("capped", Body("aa"));
    await client.PushAsync("capped", Body("bb"));
    var discarded = await client.PushAsync("capped", Body("cc"));
    if (!discarded.Discarded || discarded.Depth != 2)
    {
        throw new InvalidOperationException($"Expected a discarded push at depth 2, got discarded={discarded.Discarded} depth={discarded.Depth}.");
    }

    await client.CreateChannelAsync("rejecting", ChannelKind.Queue, new CreateChannelOptions
    {
        MaxQueueLength = 2,
        FullPolicy = FullPolicy.Reject
    });
    await client.PushAsync("rejecting", Body("a"));
    await client.PushAsync("rejecting", Body("b"));
    await ExpectErrorAsync(() => client.PushAsync("rejecting", Body("c")), ErrorCode.ChannelFull);

    await client.CreateChannelAsync("oldest", ChannelKind.Queue, new CreateChannelOptions
    {
        MaxQueueLength = 2,
        FullPolicy = FullPolicy.DiscardOldest
    });
    await client.PushAsync("oldest", Body("a"));
    await client.PushAsync("oldest", Body("b"));
    var kept = await client.PushAsync("oldest", Body("c"));
    if (kept.Discarded)
    {
        throw new InvalidOperationException("DiscardOldest stores the new message.");
    }

    var oldest = Texts(await client.PopAsync("oldest", Immediate(10)));
    Expect(oldest, "b", "c");

    await client.CreateChannelAsync("clear", ChannelKind.Queue, new CreateChannelOptions
    {
        MaxQueueLength = 2,
        FullPolicy = FullPolicy.ClearAll
    });
    await client.PushAsync("clear", Body("a"));
    await client.PushAsync("clear", Body("b"));
    await client.PushAsync("clear", Body("c"));
    Expect(Texts(await client.PopAsync("clear", Immediate(10))), "c");

    await ExpectErrorAsync(
        () => client.CreateChannelAsync("wide", ChannelKind.Queue, new CreateChannelOptions { MaxQueueLength = 9 }),
        ErrorCode.InvalidQueueLimit);
    await ExpectErrorAsync(
        () => client.CreateChannelAsync("huge", ChannelKind.Queue, new CreateChannelOptions { MaxBytes = 150 }),
        ErrorCode.InvalidByteLimit);
}

static async Task ClientCapAsync()
{
    var options = BrokerOptions();
    options.Limits.MaxMessagesPerChannel = 10;
    options.Limits.MaxMessagesPerClient = 2;
    options.Limits.FullBehavior = "Reject";
    await using var broker = await StartAsync(options);
    await using var owner = await ConnectAsync(broker);
    await using var other = await ConnectAsync(broker);
    await owner.CreateChannelAsync("a", ChannelKind.Queue);
    await owner.PushAsync("a", Body("a"));
    await owner.PushAsync("a", Body("b"));
    await ExpectErrorAsync(() => owner.PushAsync("a", Body("c")), ErrorCode.ClientQueueLimit);
    await other.PushAsync("a", Body("d"));
    Expect(Texts(await owner.PopAsync("a", Immediate(10))), "a", "b", "d");
}

static async Task MemoryCapAsync()
{
    var options = BrokerOptions();
    options.Limits.MaxMessagesPerChannel = 10;
    options.Limits.MaxQueuedBytes = 5;
    options.Limits.FullBehavior = "Reject";
    await using var broker = await StartAsync(options);
    await using var client = await ConnectAsync(broker);
    await client.CreateChannelAsync("bytes", ChannelKind.Queue);
    await client.PushAsync("bytes", new byte[] { 1, 2, 3 });
    await ExpectErrorAsync(() => client.PushAsync("bytes", new byte[] { 4, 5, 6 }), ErrorCode.ServerMemoryCap);
}

static async Task PressureAsync()
{
    var options = BrokerOptions();
    options.Limits.MaxMessagesPerChannel = 10;
    options.Limits.WarningPercent = 80;
    await using var broker = await StartAsync(options);
    var seen = new List<QueuePressure>();
    broker.QueuePressure += (_, notice) =>
    {
        if (notice.Warning && notice.Scope == "gauge")
        {
            seen.Add(notice);
        }
    };

    await using var client = await ConnectAsync(broker);
    await client.CreateChannelAsync("gauge", ChannelKind.Queue);
    for (var i = 0; i < 7; i++)
    {
        await client.PushAsync("gauge", new byte[] { (byte)i });
    }

    if (seen.Count != 0)
    {
        throw new InvalidOperationException("Pressure fired before 80%.");
    }

    await client.PushAsync("gauge", new byte[] { 7 });
    if (seen.Count != 1 || seen[0].FillPercent != 80)
    {
        throw new InvalidOperationException($"Expected one warning at 80%, got {seen.Count}.");
    }

    await client.PushAsync("gauge", new byte[] { 8 });
    if (seen.Count != 1)
    {
        throw new InvalidOperationException("Pressure fired again while the channel stayed at or above 80%.");
    }

    await client.PopAsync("gauge", Immediate(2));
    await client.PushAsync("gauge", new byte[] { 9 });
    if (seen.Count != 2 || seen[1].FillPercent != 80)
    {
        throw new InvalidOperationException("Pressure did not fire on the next upward crossing.");
    }

    var stats = broker.GetStatistics();
    var gauge = stats.Channels.Single(item => item.Name == "gauge");
    if (!gauge.WarningActive || gauge.QueuedMessages != 8 || gauge.Kind != ChannelKind.Queue)
    {
        throw new InvalidOperationException(
            $"Statistics were warning={gauge.WarningActive} queued={gauge.QueuedMessages} kind={gauge.Kind}.");
    }

    if (stats.QueuedMessages != 8)
    {
        throw new InvalidOperationException($"Broker queued count was {stats.QueuedMessages}.");
    }
}

static DomeMqOptions BrokerOptions() => new()
{
    Timeouts = { HeartbeatInterval = TimeSpan.Zero },
    Channels = { OrphanChannelTtl = TimeSpan.Zero, CleanupInterval = TimeSpan.Zero },
    Limits = { MaxPayloadSize = 64 * 1024, MaxChannels = 100, MaxChannelsPerClient = 100, MaxClients = 100 }
};

static async Task<DomeMqBroker> StartAsync(DomeMqOptions options)
{
    options.Listeners.Add(new ListenerOptions
    {
        Name = "tcp",
        Transport = "Tcp",
        Host = "127.0.0.1",
        Port = 0
    });
    var broker = new DomeMqBroker(options);
    await broker.StartAsync();
    return broker;
}

static async Task<DomeMqClient> ConnectAsync(DomeMqBroker broker)
{
    if (broker.Listeners[0].LocalEndPoint is not IPEndPoint endPoint)
    {
        throw new InvalidOperationException("The TCP listener did not bind an IP endpoint.");
    }

    return await DomeMqClient.ConnectAsync(new DomeMqClientOptions
    {
        Host = "127.0.0.1",
        Port = endPoint.Port,
        HeartbeatInterval = TimeSpan.Zero,
        PopTimeout = TimeSpan.FromSeconds(2),
        RequestTimeout = TimeSpan.FromSeconds(5)
    });
}

static async Task ExpectErrorAsync(Func<Task> call, ErrorCode code)
{
    try
    {
        await call();
    }
    catch (DomeMqException ex) when (ex.Code == code)
    {
        Console.WriteLine($"{code} ({(ushort)code})");
        return;
    }

    throw new InvalidOperationException($"Expected {code}.");
}

static PopOptions Immediate(int maxMessages) => new() { MaxMessages = maxMessages, Timeout = TimeSpan.Zero };

static byte[] Body(string text) => Encoding.UTF8.GetBytes(text);

static string[] Texts(IReadOnlyList<DomeMqMessage> messages) =>
    messages.Select(static message => Encoding.UTF8.GetString(message.Body)).ToArray();

static void Expect(IReadOnlyList<string> actual, params string[] expected)
{
    if (actual.Count != expected.Length || !actual.SequenceEqual(expected))
    {
        throw new InvalidOperationException(
            $"Expected [{string.Join(", ", expected)}] but got [{string.Join(", ", actual)}].");
    }
}
```

## TLS with SPKI pinning (PerServer)

`KeyGenerator` writes a local CA and one server certificate. The broker presents that certificate to every connection (`PerServer`). The client pins the hex SPKI SHA-256 from `servers/domemq.local.spki.sha256`. `AllowUntrustedCertificates` is set because the private CA is not in the machine trust store; the pin is what authenticates the server. A different hash is rejected. `dotnet run --project samples/TlsPinning`.

`samples/TlsPinning/Program.cs`:

```csharp
using System.Net;
using System.Text;
using DomeMQ;
using DomeMQ.Client;
using DomeMQ.Server;

var directory = Path.Combine(Path.GetTempPath(), "domemq-pin-" + Guid.NewGuid().ToString("N"));
try
{
    var generated = KeyGenerator.Generate(new KeyGenRequest
    {
        OutputDirectory = directory,
        ServerNames = ["domemq.local"],
        Format = KeyGenFormat.Pem
    });
    var hash = File.ReadAllText(Path.Combine(directory, "servers", "domemq.local.spki.sha256")).Trim();
    KeyGenerator.Generate(new KeyGenRequest
    {
        OutputDirectory = Path.Combine(directory, "other"),
        ServerNames = ["other.local"],
        Format = KeyGenFormat.Pem
    });
    var otherHash = File.ReadAllText(Path.Combine(directory, "other", "servers", "other.local.spki.sha256")).Trim();

    var options = new DomeMqOptions
    {
        Timeouts = { HeartbeatInterval = TimeSpan.Zero, HandshakeTimeout = TimeSpan.FromSeconds(5) },
        Channels = { OrphanChannelTtl = TimeSpan.Zero, CleanupInterval = TimeSpan.Zero }
    };
    options.Keys.Add(new KeyPairOptions
    {
        Id = "server",
        PrivateKey = new MaterialSource { File = Path.Combine(directory, "servers", "domemq.local.key") },
        Certificate = new MaterialSource { File = generated.ServerCertificatePaths[0] }
    });
    options.Tls.Enabled = true;
    options.Tls.KeyMode = "PerServer";
    options.Tls.KeyId = "server";
    options.Listeners.Add(new ListenerOptions
    {
        Name = "tcp",
        Transport = "Tcp",
        Host = "127.0.0.1",
        Port = 0
    });

    await using var broker = new DomeMqBroker(options);
    await broker.StartAsync();
    if (broker.Listeners[0].LocalEndPoint is not IPEndPoint endPoint)
    {
        throw new InvalidOperationException("The TCP listener did not bind an IP endpoint.");
    }

    await using var client = await DomeMqClient.ConnectAsync(Pinned(endPoint.Port, hash));
    await client.CreateChannelAsync("orders", ChannelKind.Queue);
    await client.PushAsync("orders", "pinned"u8.ToArray());
    var messages = await client.PopAsync("orders", new PopOptions { Timeout = TimeSpan.Zero });
    if (Encoding.UTF8.GetString(messages[0].Body) != "pinned")
    {
        throw new InvalidOperationException("The pinned session did not return the payload.");
    }

    Console.WriteLine($"pinned session {client.SessionId}");
    var rejected = false;
    try
    {
        await using var wrong = await DomeMqClient.ConnectAsync(Pinned(endPoint.Port, otherHash));
    }
    catch (Exception ex) when (ex.ToString().Contains("pinned", StringComparison.OrdinalIgnoreCase))
    {
        rejected = true;
    }

    if (!rejected)
    {
        throw new InvalidOperationException("A client that pinned a different key was accepted.");
    }

    Console.WriteLine("TlsPinning ok");
}
finally
{
    if (Directory.Exists(directory))
    {
        Directory.Delete(directory, true);
    }
}

static DomeMqClientOptions Pinned(int port, string spkiSha256) => new()
{
    Host = "127.0.0.1",
    Port = port,
    ClientName = "pinned",
    HeartbeatInterval = TimeSpan.Zero,
    RequestTimeout = TimeSpan.FromSeconds(10),
    Tls = new TlsClientOptions
    {
        Enabled = true,
        ServerName = "domemq.local",
        AllowUntrustedCertificates = true,
        Pin = new ServerPinOptions { SpkiSha256 = spkiSha256 }
    }
};
```

## mTLS, PerClient SNI, and several pin hashes

Two server names and a client certificate are generated. `Tls.KeyMode` is `PerClient`, so `ServerName` selects the certificate. The broker requires a client certificate signed by `ca.crt`. The worker pins both server SPKI hashes, which lets it connect to either name. A pin list that contains only the other name is rejected, and so is a handshake with no client certificate. `dotnet run --project samples/MutualTlsPerClient`.

`samples/MutualTlsPerClient/Program.cs`:

```csharp
using System.Net;
using System.Text;
using DomeMQ;
using DomeMQ.Client;
using DomeMQ.Server;

var directory = Path.Combine(Path.GetTempPath(), "domemq-mtls-" + Guid.NewGuid().ToString("N"));
try
{
    var generated = KeyGenerator.Generate(new KeyGenRequest
    {
        OutputDirectory = directory,
        ServerNames = ["alpha.dome", "beta.dome"],
        ClientNames = ["worker"],
        Format = KeyGenFormat.Pem
    });
    var alphaHash = Hash("alpha.dome");
    var betaHash = Hash("beta.dome");

    var options = new DomeMqOptions
    {
        Timeouts = { HeartbeatInterval = TimeSpan.Zero, HandshakeTimeout = TimeSpan.FromSeconds(5) },
        Channels = { OrphanChannelTtl = TimeSpan.Zero, CleanupInterval = TimeSpan.Zero }
    };
    options.Keys.Add(Key("alpha", "alpha.dome", generated.ServerCertificatePaths[0]));
    options.Keys.Add(Key("beta", "beta.dome", Path.Combine(directory, "servers", "beta.dome.crt")));
    options.Tls.Enabled = true;
    options.Tls.KeyMode = "PerClient";
    options.Tls.RequireClientCertificate = true;
    options.Tls.ClientAuthorities.Add(new MaterialSource { File = generated.CaCertificatePath });
    options.Listeners.Add(new ListenerOptions
    {
        Name = "tcp",
        Transport = "Tcp",
        Host = "127.0.0.1",
        Port = 0
    });

    await using var broker = new DomeMqBroker(options);
    await broker.StartAsync();
    if (broker.Listeners[0].LocalEndPoint is not IPEndPoint endPoint)
    {
        throw new InvalidOperationException("The TCP listener did not bind an IP endpoint.");
    }

    await using var alpha = await DomeMqClient.ConnectAsync(Worker(endPoint.Port, directory, "alpha.dome", true, alphaHash, betaHash));
    await alpha.CreateChannelAsync("orders", ChannelKind.Queue);
    await alpha.PushAsync("orders", "mutual"u8.ToArray());
    var messages = await alpha.PopAsync("orders", new PopOptions { Timeout = TimeSpan.Zero });
    if (Encoding.UTF8.GetString(messages[0].Body) != "mutual")
    {
        throw new InvalidOperationException("The mutual-TLS session did not return the payload.");
    }

    await using var beta = await DomeMqClient.ConnectAsync(Worker(endPoint.Port, directory, "beta.dome", true, alphaHash, betaHash));
    Console.WriteLine($"alpha {alpha.SessionId}, beta {beta.SessionId}");

    if (!await RejectsAsync(Worker(endPoint.Port, directory, "alpha.dome", true, betaHash)))
    {
        throw new InvalidOperationException("A pin that omitted the selected server key was accepted.");
    }

    if (!await RejectsAsync(Worker(endPoint.Port, directory, "alpha.dome", false, alphaHash, betaHash)))
    {
        throw new InvalidOperationException("A client without a certificate was accepted.");
    }

    Console.WriteLine("MutualTlsPerClient ok");
}
finally
{
    if (Directory.Exists(directory))
    {
        Directory.Delete(directory, true);
    }
}

string Hash(string name) =>
    File.ReadAllText(Path.Combine(directory, "servers", name + ".spki.sha256")).Trim();

KeyPairOptions Key(string id, string serverName, string certificate) => new()
{
    Id = id,
    PrivateKey = new MaterialSource { File = Path.Combine(directory, "servers", serverName + ".key") },
    Certificate = new MaterialSource { File = certificate },
    ServerName = serverName
};

static DomeMqClientOptions Worker(int port, string directory, string serverName, bool certificate, params string[] hashes)
{
    var pin = new ServerPinOptions();
    foreach (var hash in hashes)
    {
        pin.SpkiSha256Hashes.Add(hash);
    }

    var tls = new TlsClientOptions
    {
        Enabled = true,
        ServerName = serverName,
        AllowUntrustedCertificates = true,
        Pin = pin
    };
    if (certificate)
    {
        tls.CertificatePath = Path.Combine(directory, "clients", "worker.crt");
        tls.KeyPath = Path.Combine(directory, "clients", "worker.key");
    }

    return new DomeMqClientOptions
    {
        Host = "127.0.0.1",
        Port = port,
        ClientName = "worker",
        HeartbeatInterval = TimeSpan.Zero,
        RequestTimeout = TimeSpan.FromSeconds(10),
        Tls = tls
    };
}

static async Task<bool> RejectsAsync(DomeMqClientOptions options)
{
    try
    {
        await using var client = await DomeMqClient.ConnectAsync(options);
        return false;
    }
    catch (Exception)
    {
        return true;
    }
}
```
