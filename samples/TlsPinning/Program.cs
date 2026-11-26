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
