using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using DomeMQ.Client;

namespace DomeMQ.Server.Tests;

public class TlsKeyTests
{
    [Fact]
    public void Env_source_loads_pem_and_rejects_inline_material()
    {
        var certificate = TestCertificate.Create("env-server", server: true);
        var envCert = NewEnvName();
        var envKey = NewEnvName();
        try
        {
            Environment.SetEnvironmentVariable(envCert, certificate.CertificatePem);
            Environment.SetEnvironmentVariable(envKey, certificate.PrivateKeyPem);
            var options = OptionsWithKey("primary", new MaterialSource { Env = envKey }, new MaterialSource { Env = envCert });
            Assert.Empty(DomeMqOptionsValidator.Validate(options));
        }
        finally
        {
            Environment.SetEnvironmentVariable(envCert, null);
            Environment.SetEnvironmentVariable(envKey, null);
            certificate.Dispose();
        }

        var inline = OptionsWithKey(
            "primary",
            new MaterialSource { Env = "-----BEGIN PRIVATE KEY-----\nabc" },
            new MaterialSource { Env = "DOMEMQ_NOT_A_VALUE" });
        var errors = DomeMqOptionsValidator.Validate(inline);
        Assert.Contains(errors, error => error.Contains("Inline key material is not allowed", StringComparison.Ordinal));
    }

    [Fact]
    public void File_source_and_encrypted_pem_round_trip()
    {
        using var directory = new TempDirectory();
        var certificate = TestCertificate.Create("file-server", server: true);
        var certPath = directory.Write("server.crt", certificate.CertificatePem);
        var keyPath = directory.Write("server.key", certificate.PrivateKeyPem);
        var options = OptionsWithKey("primary", new MaterialSource { File = keyPath }, new MaterialSource { File = certPath });
        Assert.Empty(DomeMqOptionsValidator.Validate(options));

        var passwordName = NewEnvName();
        var encryptedKey = directory.Write("server.enc.key", certificate.EncryptedPrivateKeyPem("secret"));
        Environment.SetEnvironmentVariable(passwordName, "secret");
        try
        {
            var encrypted = OptionsWithKey(
                "primary",
                new MaterialSource { File = encryptedKey },
                new MaterialSource { File = certPath },
                new MaterialSource { Env = passwordName });
            Assert.Empty(DomeMqOptionsValidator.Validate(encrypted));
        }
        finally
        {
            Environment.SetEnvironmentVariable(passwordName, null);
            certificate.Dispose();
        }
    }

    [Fact]
    public void Validation_reports_missing_sources_duplicates_and_key_ids()
    {
        var both = OptionsWithKey(
            "primary",
            new MaterialSource { Env = "ONE", File = "also.pem" },
            new MaterialSource { Env = "TWO" });
        Assert.Contains(DomeMqOptionsValidator.Validate(both), error => error.Contains("exactly one of Env or File", StringComparison.Ordinal));

        var missingEnv = OptionsWithKey(
            "primary",
            new MaterialSource { Env = NewEnvName() },
            new MaterialSource { Env = NewEnvName() });
        Assert.Contains(DomeMqOptionsValidator.Validate(missingEnv), error => error.Contains("missing or empty", StringComparison.Ordinal));

        var missingFile = OptionsWithKey(
            "primary",
            new MaterialSource { File = Path.Combine(Path.GetTempPath(), $"missing-{Guid.NewGuid():N}.key") },
            new MaterialSource { File = Path.Combine(Path.GetTempPath(), $"missing-{Guid.NewGuid():N}.crt") });
        Assert.Contains(DomeMqOptionsValidator.Validate(missingFile), error => error.Contains("was not found", StringComparison.Ordinal));

        using var directory = new TempDirectory();
        using var certificate = TestCertificate.Create("dup", server: true);
        var certPath = directory.Write("a.crt", certificate.CertificatePem);
        var keyPath = directory.Write("a.key", certificate.PrivateKeyPem);
        var duplicate = BaseOptions();
        duplicate.Keys.Add(Key("same", keyPath, certPath));
        duplicate.Keys.Add(Key("same", keyPath, certPath));
        Assert.Contains(DomeMqOptionsValidator.Validate(duplicate), error => error.Contains("Duplicate key id", StringComparison.Ordinal));

        var unknown = BaseOptions();
        unknown.Keys.Add(Key("real", keyPath, certPath));
        unknown.Tls.Enabled = true;
        unknown.Tls.KeyId = "missing";
        unknown.Listeners.Add(TcpListener("tcp"));
        Assert.Contains(DomeMqOptionsValidator.Validate(unknown), error => error.Contains("does not match a Keys entry", StringComparison.Ordinal));

        var ambiguous = BaseOptions();
        ambiguous.Keys.Add(Key("one", keyPath, certPath));
        ambiguous.Keys.Add(Key("two", keyPath, certPath));
        ambiguous.Tls.Enabled = true;
        ambiguous.Listeners.Add(TcpListener("tcp"));
        Assert.Contains(DomeMqOptionsValidator.Validate(ambiguous), error => error.Contains("without KeyId", StringComparison.Ordinal));

        var none = BaseOptions();
        none.Tls.Enabled = true;
        none.Listeners.Add(TcpListener("tcp"));
        Assert.Contains(DomeMqOptionsValidator.Validate(none), error => error.Contains("no keys are configured", StringComparison.Ordinal));

        var fallback = BaseOptions();
        fallback.Keys.Add(Key("only", keyPath, certPath));
        fallback.Tls.Enabled = true;
        fallback.Listeners.Add(TcpListener("tcp"));
        Assert.Empty(DomeMqOptionsValidator.Validate(fallback));

        var garbage = BaseOptions();
        var badCert = directory.Write("bad.crt", "-----BEGIN CERTIFICATE-----\naaaa\n-----END CERTIFICATE-----");
        var badKey = directory.Write("bad.key", "-----BEGIN PRIVATE KEY-----\naaaa\n-----END PRIVATE KEY-----");
        garbage.Keys.Add(Key("bad", badKey, badCert));
        Assert.Contains(DomeMqOptionsValidator.Validate(garbage), error => error.Contains("could not be loaded", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Two_listeners_use_different_keys_and_a_new_connection_sees_a_rotated_certificate()
    {
        using var directory = new TempDirectory();
        using var first = TestCertificate.Create("listener-a", server: true);
        using var second = TestCertificate.Create("listener-b", server: true);
        using var rotated = TestCertificate.Create("listener-a-next", server: true);
        var firstCert = directory.Write("a.crt", first.CertificatePem);
        var firstKey = directory.Write("a.key", first.PrivateKeyPem);
        var secondCert = directory.Write("b.crt", second.CertificatePem);
        var secondKey = directory.Write("b.key", second.PrivateKeyPem);

        var options = BaseOptions();
        options.Keys.Add(Key("a", firstKey, firstCert));
        options.Keys.Add(Key("b", secondKey, secondCert));
        options.Listeners.Add(TcpListener("a", new TlsOptions { Enabled = true, KeyId = "a" }));
        options.Listeners.Add(TcpListener("b", new TlsOptions { Enabled = true, KeyId = "b" }));

        await using var broker = new DomeMqBroker(options);
        await broker.StartAsync();
        var portA = PortOf(broker, "a");
        var portB = PortOf(broker, "b");
        Assert.Equal(first.Thumbprint, await RemoteThumbprint(portA));
        Assert.Equal(second.Thumbprint, await RemoteThumbprint(portB));

        await using var client = await DomeMqClient.ConnectAsync(ClientOptions(portA));
        await client.CreateChannelAsync("orders", ChannelKind.Queue);
        await client.PushAsync("orders", "tls"u8.ToArray());
        var messages = await client.PopAsync("orders", new PopOptions { Timeout = TimeSpan.Zero });
        Assert.Equal("tls", Encoding.UTF8.GetString(messages[0].Body));

        directory.Write("a.crt", rotated.CertificatePem);
        directory.Write("a.key", rotated.PrivateKeyPem);
        broker.ReloadHotConfiguration(options);
        Assert.Equal(rotated.Thumbprint, await RemoteThumbprint(portA));
        Assert.Equal(second.Thumbprint, await RemoteThumbprint(portB));
    }

    [Fact]
    public async Task Mutual_tls_accepts_a_trusted_client_and_rejects_a_missing_one()
    {
        using var directory = new TempDirectory();
        using var server = TestCertificate.Create("mtls-server", server: true);
        using var clientCertificate = TestCertificate.Create("mtls-client", server: false);
        var serverCert = directory.Write("server.crt", server.CertificatePem);
        var serverKey = directory.Write("server.key", server.PrivateKeyPem);
        var clientCertPath = directory.Write("client.crt", clientCertificate.CertificatePem);
        var clientKeyPath = directory.Write("client.key", clientCertificate.PrivateKeyPem);
        var options = BaseOptions();
        options.Keys.Add(Key("server", serverKey, serverCert));
        options.Listeners.Add(TcpListener("tcp", new TlsOptions
        {
            Enabled = true,
            KeyId = "server",
            RequireClientCertificate = true,
            ClientAuthorities = [new MaterialSource { File = clientCertPath }]
        }));

        await using var broker = new DomeMqBroker(options);
        await broker.StartAsync();
        var port = PortOf(broker, "tcp");
        await AssertRejectedWithoutClientCertificate(port);

        var clientOptions = ClientOptions(port);
        clientOptions.Tls!.CertificatePath = clientCertPath;
        clientOptions.Tls.KeyPath = clientKeyPath;
        await using var client = await DomeMqClient.ConnectAsync(clientOptions);
        Assert.False(string.IsNullOrEmpty(client.SessionId));
    }

    private static DomeMqOptions OptionsWithKey(string id, MaterialSource privateKey, MaterialSource certificate, MaterialSource? password = null)
    {
        var options = BaseOptions();
        options.Keys.Add(new KeyPairOptions
        {
            Id = id,
            PrivateKey = privateKey,
            Certificate = certificate,
            Password = password
        });
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

    private static ListenerOptions TcpListener(string name, TlsOptions? tls = null) => new()
    {
        Name = name,
        Transport = "Tcp",
        Enabled = true,
        Host = "127.0.0.1",
        Port = 0,
        Tls = tls
    };

    private static KeyPairOptions Key(string id, string keyPath, string certificatePath) => new()
    {
        Id = id,
        PrivateKey = new MaterialSource { File = keyPath },
        Certificate = new MaterialSource { File = certificatePath }
    };

    private static int PortOf(DomeMqBroker broker, string name)
    {
        var endpoint = broker.Listeners.Single(listener => listener.Name == name).LocalEndPoint;
        return Assert.IsType<IPEndPoint>(endpoint).Port;
    }

    private static DomeMqClientOptions ClientOptions(int port) => new()
    {
        Host = "127.0.0.1",
        Port = port,
        HeartbeatInterval = TimeSpan.Zero,
        RequestTimeout = TimeSpan.FromSeconds(5),
        Tls = new TlsClientOptions
        {
            Enabled = true,
            AllowUntrustedCertificates = true,
            TargetHost = "localhost"
        }
    };

    private static async Task AssertRejectedWithoutClientCertificate(int port)
    {
        using var tcp = new TcpClient();
        await tcp.ConnectAsync(IPAddress.Loopback, port);
        await using var ssl = new SslStream(tcp.GetStream(), false, static (_, _, _, _) => true);
        try
        {
            await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = "localhost",
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                CertificateRevocationCheckMode = X509RevocationMode.NoCheck
            });
        }
        catch (AuthenticationException)
        {
            return;
        }

        Assert.False(ssl.IsMutuallyAuthenticated);
        var buffer = new byte[8];
        try
        {
            var read = await ssl.ReadAsync(buffer);
            Assert.Equal(0, read);
        }
        catch (IOException)
        {
        }
    }

    private static async Task<string> RemoteThumbprint(int port)
    {
        using var tcp = new TcpClient();
        await tcp.ConnectAsync(IPAddress.Loopback, port);
        await using var ssl = new SslStream(tcp.GetStream(), false, static (_, _, _, _) => true);
        await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
        {
            TargetHost = "localhost",
            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck
        });
        using var remote = new X509Certificate2(ssl.RemoteCertificate!);
        return remote.Thumbprint;
    }

    private static string NewEnvName() => "DOMEMQ_TEST_" + Guid.NewGuid().ToString("N");

    private sealed class TempDirectory : IDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), "domemq-tls-" + Guid.NewGuid().ToString("N"));

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

internal sealed class TestCertificate : IDisposable
{
    private TestCertificate(string certificatePem, string privateKeyPem, X509Certificate2 certificate, RSA key)
    {
        CertificatePem = certificatePem;
        PrivateKeyPem = privateKeyPem;
        Certificate = certificate;
        _key = key;
    }

    private readonly RSA _key;

    public string CertificatePem { get; }

    public string PrivateKeyPem { get; }

    public X509Certificate2 Certificate { get; }

    public string Thumbprint => Certificate.Thumbprint;

    public string EncryptedPrivateKeyPem(string password) =>
        _key.ExportEncryptedPkcs8PrivateKeyPem(password, new PbeParameters(PbeEncryptionAlgorithm.Aes256Cbc, HashAlgorithmName.SHA256, 10_000));

    public static TestCertificate Create(string commonName, bool server)
    {
        var key = RSA.Create(2048);
        var request = new CertificateRequest($"CN={commonName}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, false));
        var usage = server ? "1.3.6.1.5.5.7.3.1" : "1.3.6.1.5.5.7.3.2";
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid(usage)], false));
        var alternative = new SubjectAlternativeNameBuilder();
        alternative.AddDnsName("localhost");
        alternative.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(alternative.Build());
        using var issued = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(2));
        var persisted = X509CertificateLoader.LoadPkcs12(issued.Export(X509ContentType.Pkcs12), null);
        return new TestCertificate(persisted.ExportCertificatePem(), key.ExportPkcs8PrivateKeyPem(), persisted, key);
    }

    public void Dispose()
    {
        Certificate.Dispose();
        _key.Dispose();
    }
}
