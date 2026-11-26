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
