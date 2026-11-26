using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using DomeMQ;
using DomeMQ.Client;

namespace DomeMQ.Server.Tests;

public class KeyModeTests
{
    [Fact]
    public void Validation_rejects_duplicate_names_unknown_fallback_and_keeps_per_server()
    {
        using var directory = new TempDirectory();
        using var certificate = TestCertificate.Create("names", server: true);
        var certPath = directory.Write("a.crt", certificate.CertificatePem);
        var keyPath = directory.Write("a.key", certificate.PrivateKeyPem);
        var options = BaseOptions();
        options.Keys.Add(Named("alpha", keyPath, certPath, "alpha.dome"));
        options.Keys.Add(Named("beta", keyPath, certPath, "alpha.dome"));
        options.Tls.Enabled = true;
        options.Tls.KeyMode = "PerClient";
        options.Listeners.Add(Listener("tcp"));
        Assert.Contains(DomeMqOptionsValidator.Validate(options), error => error.Contains("Duplicate server name", StringComparison.Ordinal));

        options.Keys[1].ServerName = "beta.dome";
        options.Tls.FallbackKeyId = "missing";
        Assert.Contains(DomeMqOptionsValidator.Validate(options), error => error.Contains("FallbackKeyId", StringComparison.Ordinal));

        options.Tls.FallbackKeyId = null;
        options.Tls.KeyId = "alpha";
        Assert.Contains(DomeMqOptionsValidator.Validate(options), error => error.Contains("Remove KeyId", StringComparison.Ordinal));

        options.Tls.KeyMode = "PerServer";
        options.Tls.KeyId = "alpha";
        options.Tls.FallbackKeyId = "beta";
        Assert.Contains(DomeMqOptionsValidator.Validate(options), error => error.Contains("only used when KeyMode is PerClient", StringComparison.Ordinal));

        options.Tls.FallbackKeyId = null;
        Assert.Empty(DomeMqOptionsValidator.Validate(options));
    }

    [Fact]
    public async Task Two_clients_on_one_port_get_different_keys_and_a_wrong_pin_is_rejected()
    {
        using var directory = new TempDirectory();
        using var alpha = TestCertificate.Create("alpha.dome", server: true);
        using var beta = TestCertificate.Create("beta.dome", server: true);
        var options = Listening(directory, alpha, beta, fallback: null);
        var seen = new RecordingKeys();
        await using var broker = new DomeMqBroker(options) { ConnectionObserver = seen };
        await broker.StartAsync();
        var port = PortOf(broker);

        await using var alphaClient = await DomeMqClient.ConnectAsync(Client(port, "alpha.dome", alpha.Certificate));
        await using var betaClient = await DomeMqClient.ConnectAsync(Client(port, "beta.dome", beta.Certificate));
        Assert.Equal(alpha.Thumbprint, await RemoteThumbprint(port, "alpha.dome"));
        Assert.Equal(beta.Thumbprint, await RemoteThumbprint(port, "beta.dome"));
        Assert.Contains(seen.Keys, id => id == "alpha");
        Assert.Contains(seen.Keys, id => id == "beta");

        var wrong = Client(port, "alpha.dome", beta.Certificate);
        var rejected = await Assert.ThrowsAnyAsync<Exception>(() => DomeMqClient.ConnectAsync(wrong));
        Assert.Contains("pinned", rejected.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Unknown_sni_is_rejected_unless_a_fallback_key_is_configured()
    {
        using var directory = new TempDirectory();
        using var alpha = TestCertificate.Create("alpha.dome", server: true);
        using var beta = TestCertificate.Create("beta.dome", server: true);
        var strict = Listening(directory, alpha, beta, fallback: null);
        await using (var broker = new DomeMqBroker(strict))
        {
            await broker.StartAsync();
            var port = PortOf(broker);
            await AssertRejected(port, "other.dome");
            await AssertRejected(port, null);
        }

        var relaxed = Listening(directory, alpha, beta, fallback: "beta");
        await using var fallbackBroker = new DomeMqBroker(relaxed);
        await fallbackBroker.StartAsync();
        Assert.Equal(beta.Thumbprint, await RemoteThumbprint(PortOf(fallbackBroker), "other.dome"));
    }

    [Fact]
    public async Task A_pin_accepts_either_of_two_spki_hashes()
    {
        using var directory = new TempDirectory();
        using var current = TestCertificate.Create("only.dome", server: true);
        using var next = TestCertificate.Create("next.dome", server: true);
        var certPath = directory.Write("only.crt", current.CertificatePem);
        var keyPath = directory.Write("only.key", current.PrivateKeyPem);
        var options = BaseOptions();
        options.Keys.Add(Named("only", keyPath, certPath, "only.dome"));
        options.Tls.Enabled = true;
        options.Tls.KeyMode = "PerServer";
        options.Tls.KeyId = "only";
        options.Listeners.Add(Listener("tcp"));
        await using var broker = new DomeMqBroker(options);
        await broker.StartAsync();
        var port = PortOf(broker);
        var currentHash = Convert.ToHexString(CertificatePin.SpkiSha256(current.Certificate)).ToLowerInvariant();
        var nextHash = Convert.ToHexString(CertificatePin.SpkiSha256(next.Certificate)).ToLowerInvariant();

        var overlap = Client(port, "only.dome", certificate: null);
        overlap.Tls!.Pin = new ServerPinOptions { SpkiSha256 = currentHash + " " + nextHash };
        await using var connected = await DomeMqClient.ConnectAsync(overlap);
        Assert.False(string.IsNullOrEmpty(connected.SessionId));

        var onlyNext = Client(port, "only.dome", certificate: null);
        onlyNext.Tls!.Pin = new ServerPinOptions { SpkiSha256Hashes = [nextHash] };
        var rejected = await Assert.ThrowsAnyAsync<Exception>(() => DomeMqClient.ConnectAsync(onlyNext));
        Assert.Contains("pinned", rejected.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Per_server_ignores_the_client_server_name()
    {
        using var directory = new TempDirectory();
        using var certificate = TestCertificate.Create("only.dome", server: true);
        var certPath = directory.Write("only.crt", certificate.CertificatePem);
        var keyPath = directory.Write("only.key", certificate.PrivateKeyPem);
        var options = BaseOptions();
        options.Keys.Add(Named("only", keyPath, certPath, "only.dome"));
        options.Tls.Enabled = true;
        options.Tls.KeyMode = "PerServer";
        options.Tls.KeyId = "only";
        options.Listeners.Add(Listener("tcp"));
        await using var broker = new DomeMqBroker(options);
        await broker.StartAsync();
        Assert.Equal(certificate.Thumbprint, await RemoteThumbprint(PortOf(broker), "someone-else.dome"));
    }

    [Fact]
    public async Task Keygen_output_serves_two_names_and_mutual_tls()
    {
        var output = Path.Combine(Path.GetTempPath(), "domemq-keygen-" + Guid.NewGuid().ToString("N"));
        try
        {
            var generated = KeyGenerator.Generate(new KeyGenRequest
            {
                OutputDirectory = output,
                ServerNames = ["alpha.dome", "beta.dome"],
                ClientNames = ["worker"],
                Format = KeyGenFormat.Both
            });
            Assert.True(File.Exists(Path.Combine(output, "servers", "alpha.dome.spki.sha256")));
            Assert.True(File.Exists(Path.Combine(output, "servers", "alpha.dome.pfx")));
            Assert.True(File.Exists(Path.Combine(output, "clients", "worker.crt")));

            var alphaSpki = File.ReadAllText(Path.Combine(output, "servers", "alpha.dome.spki.sha256")).Trim();
            var options = BaseOptions();
            options.Keys.Add(Named("alpha", Path.Combine(output, "servers", "alpha.dome.key"), generated.ServerCertificatePaths[0], "alpha.dome"));
            options.Keys.Add(Named("beta", Path.Combine(output, "servers", "beta.dome.key"), Path.Combine(output, "servers", "beta.dome.crt"), "beta.dome"));
            options.Tls.Enabled = true;
            options.Tls.KeyMode = "PerClient";
            options.Tls.RequireClientCertificate = true;
            options.Tls.ClientAuthorities = [new MaterialSource { File = generated.CaCertificatePath }];
            options.Listeners.Add(Listener("tcp"));
            await using var broker = new DomeMqBroker(options);
            await broker.StartAsync();
            var port = PortOf(broker);

            var client = Client(port, "alpha.dome", certificate: null);
            client.Tls!.Pin = new ServerPinOptions { SpkiSha256 = alphaSpki };
            client.Tls.CertificatePath = Path.Combine(output, "clients", "worker.crt");
            client.Tls.KeyPath = Path.Combine(output, "clients", "worker.key");
            await using var connected = await DomeMqClient.ConnectAsync(client);
            await connected.CreateChannelAsync("orders", ChannelKind.Queue);
            await connected.PushAsync("orders", "keygen"u8.ToArray());
            var messages = await connected.PopAsync("orders", new PopOptions { Timeout = TimeSpan.Zero });
            Assert.Equal("keygen", Encoding.UTF8.GetString(messages[0].Body));

            var stranger = Client(port, "alpha.dome", certificate: null);
            stranger.Tls!.Pin = new ServerPinOptions { SpkiSha256 = alphaSpki };
            await Assert.ThrowsAnyAsync<Exception>(() => DomeMqClient.ConnectAsync(stranger));
        }
        finally
        {
            if (Directory.Exists(output))
            {
                Directory.Delete(output, recursive: true);
            }
        }
    }

    [Fact]
    public async Task Reloading_a_per_client_certificate_changes_the_next_handshake()
    {
        using var directory = new TempDirectory();
        using var alpha = TestCertificate.Create("alpha.dome", server: true);
        using var next = TestCertificate.Create("alpha.dome", server: true);
        using var beta = TestCertificate.Create("beta.dome", server: true);
        var options = Listening(directory, alpha, beta, fallback: null);
        await using var broker = new DomeMqBroker(options);
        await broker.StartAsync();
        var port = PortOf(broker);
        Assert.Equal(alpha.Thumbprint, await RemoteThumbprint(port, "alpha.dome"));
        directory.Write("alpha.crt", next.CertificatePem);
        directory.Write("alpha.key", next.PrivateKeyPem);
        broker.ReloadHotConfiguration(options);
        Assert.Equal(next.Thumbprint, await RemoteThumbprint(port, "alpha.dome"));
        Assert.Equal(beta.Thumbprint, await RemoteThumbprint(port, "beta.dome"));
    }

    private static DomeMqOptions Listening(TempDirectory directory, TestCertificate alpha, TestCertificate beta, string? fallback)
    {
        var alphaCert = directory.Write("alpha.crt", alpha.CertificatePem);
        var alphaKey = directory.Write("alpha.key", alpha.PrivateKeyPem);
        var betaCert = directory.Write("beta.crt", beta.CertificatePem);
        var betaKey = directory.Write("beta.key", beta.PrivateKeyPem);
        var options = BaseOptions();
        options.Keys.Add(Named("alpha", alphaKey, alphaCert, "alpha.dome"));
        options.Keys.Add(Named("beta", betaKey, betaCert, "beta.dome"));
        options.Tls.Enabled = true;
        options.Tls.KeyMode = "PerClient";
        options.Tls.FallbackKeyId = fallback;
        options.Listeners.Add(Listener("tcp"));
        return options;
    }

    private static DomeMqOptions BaseOptions() => new()
    {
        Timeouts =
        {
            HeartbeatInterval = TimeSpan.Zero,
            IdleTimeout = TimeSpan.FromMinutes(1),
            HandshakeTimeout = TimeSpan.FromSeconds(5),
            ReadTimeout = TimeSpan.FromSeconds(5),
            WriteTimeout = TimeSpan.FromSeconds(5)
        },
        Channels = { OrphanChannelTtl = TimeSpan.Zero, CleanupInterval = TimeSpan.Zero }
    };

    private static ListenerOptions Listener(string name) => new()
    {
        Name = name,
        Transport = "Tcp",
        Enabled = true,
        Host = "127.0.0.1",
        Port = 0
    };

    private static KeyPairOptions Named(string id, string keyPath, string certificatePath, string serverName) => new()
    {
        Id = id,
        PrivateKey = new MaterialSource { File = keyPath },
        Certificate = new MaterialSource { File = certificatePath },
        ServerName = serverName
    };

    private static int PortOf(DomeMqBroker broker) =>
        Assert.IsType<IPEndPoint>(broker.Listeners[0].LocalEndPoint).Port;

    private static DomeMqClientOptions Client(int port, string serverName, X509Certificate2? certificate) => new()
    {
        Host = "127.0.0.1",
        Port = port,
        HeartbeatInterval = TimeSpan.Zero,
        RequestTimeout = TimeSpan.FromSeconds(5),
        Tls = new TlsClientOptions
        {
            Enabled = true,
            AllowUntrustedCertificates = true,
            ServerName = serverName,
            Pin = certificate is null
                ? null
                : new ServerPinOptions { SpkiSha256Bytes = CertificatePin.SpkiSha256(certificate) }
        }
    };

    private static async Task AssertRejected(int port, string? serverName)
    {
        var error = await Record.ExceptionAsync(() => RemoteThumbprint(port, serverName));
        Assert.NotNull(error);
        Assert.True(error is AuthenticationException or IOException or DomeMqException);
    }

    private static async Task<string> RemoteThumbprint(int port, string? serverName)
    {
        using var tcp = new TcpClient();
        await tcp.ConnectAsync(IPAddress.Loopback, port);
        await using var ssl = new SslStream(tcp.GetStream(), false, static (_, _, _, _) => true);
        await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
        {
            TargetHost = serverName ?? "",
            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck
        });
        using var remote = new X509Certificate2(ssl.RemoteCertificate!);
        return remote.Thumbprint;
    }

    private sealed class RecordingKeys : IBrokerConnectionObserver
    {
        public List<string?> Keys { get; } = [];

        public void OnConnected(BrokerConnectionSnapshot snapshot) => Keys.Add(snapshot.KeyId);

        public void OnDisconnected(string id)
        {
        }
    }

    private sealed class TempDirectory : IDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), "domemq-keys-" + Guid.NewGuid().ToString("N"));

        public TempDirectory() => Directory.CreateDirectory(_path);

        public string Write(string name, string contents)
        {
            var path = Path.Combine(_path, name);
            File.WriteAllText(path, contents);
            return path;
        }

        public void Dispose() => Directory.Delete(_path, recursive: true);
    }
}
