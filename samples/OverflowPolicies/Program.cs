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
