using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace DomeMQ;

/// <summary>A connected byte stream. Implementations must be safe for one reader and one writer.</summary>
public interface IByteTransport : IAsyncDisposable
{
    /// <summary>Remote IP when the transport is IP-based. Null for Unix sockets and other local transports.</summary>
    IPAddress? RemoteAddress { get; }

    string Description { get; }

    ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken);

    /// <summary>Reads at least one byte, or returns 0 when the peer has closed.</summary>
    ValueTask<int> ReceiveAsync(Memory<byte> buffer, CancellationToken cancellationToken);

    /// <summary>Subject of the peer certificate after a TLS handshake. Null when the connection is not TLS or the peer sent no certificate.</summary>
    string? PeerCertificateSubject { get; }

    /// <summary>Server key id chosen for this connection. Null on plaintext and on client sockets.</summary>
    string? ServerKeyId { get; }
}

/// <summary>A bound listener that accepts <see cref="IByteTransport"/> connections.</summary>
public interface ITransportListener : IAsyncDisposable
{
    EndPoint? LocalEndPoint { get; }

    string Description { get; }

    ValueTask<IByteTransport> AcceptAsync(CancellationToken cancellationToken);
}

/// <summary>
/// Pluggable transport. TCP and Unix domain sockets are implemented.
/// UDP, KCP, and shared memory are registered and throw <see cref="TransportNotSupportedException"/>.
/// </summary>
public interface ITransportFactory
{
    string Name { get; }

    bool IsImplemented { get; }

    ITransportListener Listen(TransportSettings settings);

    ValueTask<IByteTransport> ConnectAsync(TransportSettings settings, CancellationToken cancellationToken);
}

/// <summary>Socket and TLS settings shared by listeners and outbound connections.</summary>
public sealed class TransportSettings
{
    public string Transport { get; init; } = "tcp";

    public string Host { get; init; } = "127.0.0.1";

    public int Port { get; init; }

    public string? Path { get; init; }

    public bool NoDelay { get; init; } = true;

    public bool KeepAlive { get; init; } = true;

    public int KeepAliveTimeSeconds { get; init; } = 60;

    public int KeepAliveIntervalSeconds { get; init; } = 10;

    public int ReceiveBufferSize { get; init; } = 256 * 1024;

    public int SendBufferSize { get; init; } = 256 * 1024;

    public int Backlog { get; init; } = 512;

    public int? LingerSeconds { get; init; }

    public bool DeleteStaleSocket { get; init; } = true;

    /// <summary>Unix permission bits, for example <c>0o660</c>. Null leaves the platform default.</summary>
    public int? UnixPermissions { get; init; }

    public TlsParameters? Tls { get; init; }

    /// <summary>Optional tap. Each partial read and write is reported.</summary>
    public ITrafficObserver? TrafficObserver { get; init; }
}

/// <summary>TLS parameters. Used for TCP only.</summary>
public sealed class TlsParameters
{
    public bool Enabled { get; init; }

    public X509Certificate2? Certificate { get; init; }

    /// <summary>Invoked for each accepted connection so the server certificate can be rotated without a restart.</summary>
    public Func<TlsListenerMaterial>? HandshakeSelector { get; init; }

    /// <summary>SHA-256 hashes of acceptable server SubjectPublicKeyInfo values. The handshake fails when the peer key matches none of them.</summary>
    public IReadOnlyList<byte[]>? PinnedSpkiSha256 { get; init; }

    /// <summary>Expected server certificate. The handshake fails when the peer certificate thumbprint differs.</summary>
    public X509Certificate2? PinnedCertificate { get; init; }

    public bool RequireClientCertificate { get; init; }

    public bool AllowUntrustedCertificates { get; init; }

    public SslProtocols Protocols { get; init; } = SslProtocols.Tls12 | SslProtocols.Tls13;

    public string? TargetHost { get; init; }

    public X509Certificate2? ClientCertificate { get; init; }
}

/// <summary>Certificates a listener can present. <see cref="Fixed"/> is one key for every connection. <see cref="ByName"/> selects by the TLS server name.</summary>
public sealed class TlsListenerMaterial
{
    public TlsHandshake? Fixed { get; init; }

    public IReadOnlyDictionary<string, TlsHandshake>? ByName { get; init; }

    public TlsHandshake? Fallback { get; init; }
}

/// <summary>Server TLS settings for a single accepted connection.</summary>
public sealed class TlsHandshake
{
    public string KeyId { get; init; } = "";

    public required X509Certificate2 ServerCertificate { get; init; }

    public X509Certificate2Collection? AdditionalCertificates { get; init; }

    public bool RequireClientCertificate { get; init; }

    public bool AllowUntrustedClientCertificates { get; init; }

    public SslProtocols Protocols { get; init; } = SslProtocols.Tls12 | SslProtocols.Tls13;

    public X509Certificate2Collection? TrustedClientRoots { get; init; }
}

public static class TransportRegistry
{
    public static ITransportFactory Resolve(string name) =>
        Normalize(name) switch
        {
            "tcp" => new TcpTransportFactory(),
            "unix" => new UnixSocketTransportFactory(),
            "udp" => new UdpTransportFactory(),
            "kcp" => new KcpTransportFactory(),
            "shm" => new SharedMemoryTransportFactory(),
            _ => throw new ArgumentException($"Unknown transport '{name}'.", nameof(name))
        };

    public static bool IsKnown(string name)
    {
        var normalized = Normalize(name);
        return normalized is "tcp" or "unix" or "udp" or "kcp" or "shm";
    }

    public static bool IsImplemented(string name) => Normalize(name) is "tcp" or "unix";

    /// <summary>
    /// IP access control applies to IP transports only.
    /// Unix domain sockets and shared memory are not subject to IP rules.
    /// </summary>
    public static bool AppliesIpAccess(string name) => Normalize(name) is "tcp" or "udp";

    public static string Normalize(string name) =>
        name.Trim().ToLowerInvariant() switch
        {
            "tcp" => "tcp",
            "unix" or "unixsocket" or "uds" => "unix",
            "udp" => "udp",
            "kcp" => "kcp",
            "shm" or "sharedmemory" => "shm",
            _ => name.Trim().ToLowerInvariant()
        };
}

public sealed class TcpTransportFactory : ITransportFactory
{
    public string Name => "tcp";

    public bool IsImplemented => true;

    public ITransportListener Listen(TransportSettings settings) => new TcpListener(settings);

    public async ValueTask<IByteTransport> ConnectAsync(TransportSettings settings, CancellationToken cancellationToken)
    {
        var address = IPAddress.Parse(settings.Host);
        var socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            SocketSetup.ApplyTcp(socket, settings);
            await socket.ConnectAsync(new IPEndPoint(address, settings.Port), cancellationToken).ConfigureAwait(false);
            return await SocketSetup.WrapClientAsync(socket, settings, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }
}

public sealed class UnixSocketTransportFactory : ITransportFactory
{
    public string Name => "unix";

    public bool IsImplemented => true;

    public ITransportListener Listen(TransportSettings settings) => new UnixListener(settings);

    public async ValueTask<IByteTransport> ConnectAsync(TransportSettings settings, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(settings.Path))
        {
            throw new ArgumentException("Unix socket path is required.", nameof(settings));
        }

        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        try
        {
            SocketSetup.ApplyBuffers(socket, settings);
            await socket.ConnectAsync(new UnixDomainSocketEndPoint(settings.Path), cancellationToken).ConfigureAwait(false);
            return new SocketByteTransport(socket, remoteAddress: null, $"unix://{settings.Path}", settings.TrafficObserver);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }
}

public sealed class UdpTransportFactory : ITransportFactory
{
    public string Name => "udp";

    public bool IsImplemented => false;

    public ITransportListener Listen(TransportSettings settings) => throw NotSupported();

    public ValueTask<IByteTransport> ConnectAsync(TransportSettings settings, CancellationToken cancellationToken) =>
        throw NotSupported();

    private static TransportNotSupportedException NotSupported() =>
        new("UDP transport is not implemented. A future datagram transport should carry one DomeMQ frame per datagram and register itself through ITransportFactory.");
}

public sealed class KcpTransportFactory : ITransportFactory
{
    public string Name => "kcp";

    public bool IsImplemented => false;

    public ITransportListener Listen(TransportSettings settings) => throw NotSupported();

    public ValueTask<IByteTransport> ConnectAsync(TransportSettings settings, CancellationToken cancellationToken) =>
        throw NotSupported();

    private static TransportNotSupportedException NotSupported() =>
        new("KCP transport is not implemented. A future reliable-UDP transport should present the same byte stream of DomeMQ frames.");
}

public sealed class SharedMemoryTransportFactory : ITransportFactory
{
    public string Name => "shm";

    public bool IsImplemented => false;

    public ITransportListener Listen(TransportSettings settings) => throw NotSupported();

    public ValueTask<IByteTransport> ConnectAsync(TransportSettings settings, CancellationToken cancellationToken) =>
        throw NotSupported();

    private static TransportNotSupportedException NotSupported() =>
        new("Shared memory transport is not implemented. A future transport can use a named memory-mapped ring plus a cross-process signal, still carrying DomeMQ frames.");
}

internal static class SocketSetup
{
    public static void ApplyTcp(Socket socket, TransportSettings settings)
    {
        socket.NoDelay = settings.NoDelay;
        ApplyBuffers(socket, settings);
        if (settings.LingerSeconds is int linger)
        {
            socket.LingerState = new LingerOption(enable: true, linger >= 0 ? linger : 0);
        }

        if (!settings.KeepAlive)
        {
            return;
        }

        socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
        try
        {
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, Math.Max(1, settings.KeepAliveTimeSeconds));
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, Math.Max(1, settings.KeepAliveIntervalSeconds));
        }
        catch (SocketException)
        {
            // Some platforms reject the TCP keepalive timing options. The SO_KEEPALIVE flag still stands.
        }
    }

    public static void ApplyBuffers(Socket socket, TransportSettings settings)
    {
        if (settings.ReceiveBufferSize > 0)
        {
            socket.ReceiveBufferSize = settings.ReceiveBufferSize;
        }

        if (settings.SendBufferSize > 0)
        {
            socket.SendBufferSize = settings.SendBufferSize;
        }
    }

    public static async ValueTask<IByteTransport> WrapClientAsync(
        Socket socket,
        TransportSettings settings,
        CancellationToken cancellationToken)
    {
        var remote = (socket.RemoteEndPoint as IPEndPoint)?.Address;
        var description = DescribeTcp(socket.RemoteEndPoint);
        if (settings.Tls is not { Enabled: true } tls)
        {
            return new SocketByteTransport(socket, remote, description, settings.TrafficObserver);
        }

        var stream = new NetworkStream(socket, ownsSocket: true);
        string? pinFailure = null;
        var pin = tls.PinnedSpkiSha256 is { Count: > 0 } || tls.PinnedCertificate is not null;
        RemoteCertificateValidationCallback? validate = pin
            ? (_, certificate, _, errors) =>
            {
                if (!CertificatePin.AcceptServer(certificate, errors, tls.AllowUntrustedCertificates, tls.PinnedSpkiSha256, tls.PinnedCertificate, out var reason))
                {
                    pinFailure = reason;
                    return false;
                }

                return true;
            }
            : tls.AllowUntrustedCertificates ? static (_, _, _, _) => true : null;
        var ssl = new SslStream(stream, leaveInnerStreamOpen: false, validate);
        var certificates = new X509CertificateCollection();
        if (tls.ClientCertificate is not null)
        {
            certificates.Add(tls.ClientCertificate);
        }

        try
        {
            await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
            {
                TargetHost = string.IsNullOrWhiteSpace(tls.TargetHost) ? settings.Host : tls.TargetHost,
                EnabledSslProtocols = tls.Protocols,
                CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                ClientCertificates = certificates
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await ssl.DisposeAsync().ConfigureAwait(false);
            if (pinFailure is not null)
            {
                throw new DomeMqException(pinFailure, inner: ex);
            }

            throw;
        }

        return new StreamByteTransport(ssl, remote, description + "+tls", settings.TrafficObserver, ssl.RemoteCertificate?.Subject);
    }

    public static async ValueTask<IByteTransport> WrapServerAsync(
        Socket socket,
        TransportSettings settings,
        CancellationToken cancellationToken)
    {
        var remote = (socket.RemoteEndPoint as IPEndPoint)?.Address;
        var description = DescribeTcp(socket.RemoteEndPoint);
        if (settings.Tls is not { Enabled: true } tls)
        {
            return new SocketByteTransport(socket, remote, description, settings.TrafficObserver);
        }

        var material = tls.HandshakeSelector?.Invoke();
        var stream = new NetworkStream(socket, ownsSocket: true);
        var ssl = new SslStream(stream, leaveInnerStreamOpen: false);
        string? serverKeyId = null;
        try
        {
            if (material?.ByName is not null)
            {
                var sample = material.Fallback ?? material.ByName.Values.FirstOrDefault();
                var options = new SslServerAuthenticationOptions
                {
                    ClientCertificateRequired = sample?.RequireClientCertificate ?? false,
                    EnabledSslProtocols = sample?.Protocols ?? tls.Protocols,
                    CertificateRevocationCheckMode = X509RevocationMode.NoCheck
                };
                options.ServerCertificateSelectionCallback = (_, host) =>
                {
                    var selected = SelectHandshake(material, host);
                    serverKeyId = selected.KeyId;
                    options.ServerCertificateContext = selected.AdditionalCertificates is { Count: > 0 } extras
                        ? SslStreamCertificateContext.Create(selected.ServerCertificate, extras)
                        : null;
                    return selected.ServerCertificate;
                };
                if (sample?.RequireClientCertificate == true)
                {
                    var roots = sample.TrustedClientRoots;
                    var allowUntrusted = sample.AllowUntrustedClientCertificates;
                    options.RemoteCertificateValidationCallback = (_, clientCertificate, _, _) =>
                        ValidateClientCertificate(clientCertificate, allowUntrusted, roots);
                }

                await ssl.AuthenticateAsServerAsync(options, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var handshake = material?.Fixed;
                var certificate = handshake?.ServerCertificate ?? tls.Certificate;
                if (certificate is null)
                {
                    throw new InvalidOperationException("TLS is enabled but no server certificate was loaded.");
                }

                handshake ??= new TlsHandshake
                {
                    ServerCertificate = certificate,
                    RequireClientCertificate = tls.RequireClientCertificate,
                    AllowUntrustedClientCertificates = tls.AllowUntrustedCertificates,
                    Protocols = tls.Protocols
                };
                serverKeyId = handshake.KeyId;
                await ssl.AuthenticateAsServerAsync(BuildServerOptions(handshake), cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            await ssl.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return new StreamByteTransport(ssl, remote, description + "+tls", settings.TrafficObserver, ssl.RemoteCertificate?.Subject, serverKeyId);
    }

    private static TlsHandshake SelectHandshake(TlsListenerMaterial material, string? serverName)
    {
        if (!string.IsNullOrWhiteSpace(serverName) && material.ByName!.TryGetValue(serverName, out var match))
        {
            return match;
        }

        if (material.Fallback is not null)
        {
            return material.Fallback;
        }

        var shown = string.IsNullOrWhiteSpace(serverName) ? "missing" : serverName;
        throw new AuthenticationException($"No server certificate for SNI '{shown}'.");
    }

    private static SslServerAuthenticationOptions BuildServerOptions(TlsHandshake handshake)
    {
        var options = new SslServerAuthenticationOptions
        {
            ServerCertificate = handshake.ServerCertificate,
            ClientCertificateRequired = handshake.RequireClientCertificate,
            EnabledSslProtocols = handshake.Protocols,
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck
        };
        if (handshake.AdditionalCertificates is { Count: > 0 } extras)
        {
            options.ServerCertificateContext = SslStreamCertificateContext.Create(handshake.ServerCertificate, extras);
        }

        if (handshake.RequireClientCertificate)
        {
            var roots = handshake.TrustedClientRoots;
            var allowUntrusted = handshake.AllowUntrustedClientCertificates;
            options.RemoteCertificateValidationCallback = (_, clientCertificate, _, _) =>
                ValidateClientCertificate(clientCertificate, allowUntrusted, roots);
        }

        return options;
    }

    public static string DescribeTcp(EndPoint? endpoint) =>
        endpoint is IPEndPoint ip ? $"tcp://{ip.Address}:{ip.Port}" : "tcp://unknown";

    private static bool ValidateClientCertificate(
        X509Certificate? certificate,
        bool allowUntrusted,
        X509Certificate2Collection? trustedRoots)
    {
        if (certificate is null)
        {
            return false;
        }

        if (allowUntrusted)
        {
            return true;
        }

        if (trustedRoots is null || trustedRoots.Count == 0)
        {
            return false;
        }

        try
        {
            using var client = new X509Certificate2(certificate);
            foreach (var root in trustedRoots)
            {
                if (string.Equals(client.Thumbprint, root.Thumbprint, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }

                if (client.GetPublicKey().AsSpan().SequenceEqual(root.GetPublicKey()))
                {
                    return true;
                }
            }

            using var chain = new X509Chain();
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            chain.ChainPolicy.CustomTrustStore.AddRange(trustedRoots);
            return chain.Build(client);
        }
        catch (CryptographicException)
        {
            return false;
        }
    }
}

internal sealed class TcpListener : ITransportListener
{
    private readonly Socket _socket;
    private readonly TransportSettings _settings;

    public TcpListener(TransportSettings settings)
    {
        _settings = settings;
        var address = IPAddress.Parse(settings.Host);
        _socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            if (address.AddressFamily == AddressFamily.InterNetworkV6)
            {
                _socket.DualMode = false;
            }

            _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            SocketSetup.ApplyTcp(_socket, settings);
            _socket.Bind(new IPEndPoint(address, settings.Port));
            _socket.Listen(Math.Max(1, settings.Backlog));
            LocalEndPoint = _socket.LocalEndPoint;
            Description = SocketSetup.DescribeTcp(LocalEndPoint);
        }
        catch
        {
            _socket.Dispose();
            throw;
        }
    }

    public EndPoint? LocalEndPoint { get; }

    public string Description { get; }

    public async ValueTask<IByteTransport> AcceptAsync(CancellationToken cancellationToken)
    {
        var accepted = await _socket.AcceptAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            SocketSetup.ApplyTcp(accepted, _settings);
            return await SocketSetup.WrapServerAsync(accepted, _settings, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            accepted.Dispose();
            throw;
        }
    }

    public ValueTask DisposeAsync()
    {
        _socket.Dispose();
        return ValueTask.CompletedTask;
    }
}

internal sealed class UnixListener : ITransportListener
{
    private readonly Socket _socket;
    private readonly string _path;
    private readonly TransportSettings _settings;

    public UnixListener(TransportSettings settings)
    {
        _settings = settings;
        if (string.IsNullOrWhiteSpace(settings.Path))
        {
            throw new ArgumentException("Unix socket path is required.", nameof(settings));
        }

        _path = settings.Path;
        if (settings.DeleteStaleSocket && File.Exists(_path))
        {
            File.Delete(_path);
        }

        _socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        try
        {
            SocketSetup.ApplyBuffers(_socket, settings);
            var endpoint = new UnixDomainSocketEndPoint(_path);
            _socket.Bind(endpoint);
            _socket.Listen(Math.Max(1, settings.Backlog));
            LocalEndPoint = _socket.LocalEndPoint;
            Description = $"unix://{_path}";
            if (!OperatingSystem.IsWindows() && settings.UnixPermissions is int mode)
            {
                File.SetUnixFileMode(_path, (UnixFileMode)mode);
            }
        }
        catch
        {
            _socket.Dispose();
            throw;
        }
    }

    public EndPoint? LocalEndPoint { get; }

    public string Description { get; }

    public async ValueTask<IByteTransport> AcceptAsync(CancellationToken cancellationToken)
    {
        var accepted = await _socket.AcceptAsync(cancellationToken).ConfigureAwait(false);
        return new SocketByteTransport(accepted, remoteAddress: null, Description, _settings.TrafficObserver);
    }

    public ValueTask DisposeAsync()
    {
        _socket.Dispose();
        if (File.Exists(_path))
        {
            try
            {
                File.Delete(_path);
            }
            catch (IOException)
            {
            }
        }

        return ValueTask.CompletedTask;
    }
}

internal sealed class SocketByteTransport : IByteTransport
{
    private readonly Socket _socket;

    private readonly ITrafficObserver? _observer;

    public SocketByteTransport(Socket socket, IPAddress? remoteAddress, string description, ITrafficObserver? observer = null)
    {
        _socket = socket;
        RemoteAddress = remoteAddress;
        Description = description;
        _observer = observer;
    }

    public IPAddress? RemoteAddress { get; }

    public string Description { get; }

    public string? PeerCertificateSubject => null;

    public string? ServerKeyId => null;

    public async ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        var sent = 0;
        while (sent < data.Length)
        {
            var count = await _socket.SendAsync(data[sent..], SocketFlags.None, cancellationToken).ConfigureAwait(false);
            if (count == 0)
            {
                throw new IOException("Socket send returned zero bytes.");
            }

            Observe(TrafficDirection.Outbound, data.Span.Slice(sent, count));
            sent += count;
        }
    }

    public async ValueTask<int> ReceiveAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var count = await _socket.ReceiveAsync(buffer, SocketFlags.None, cancellationToken).ConfigureAwait(false);
        if (count > 0)
        {
            Observe(TrafficDirection.Inbound, buffer.Span[..count]);
        }

        return count;
    }

    private void Observe(TrafficDirection direction, ReadOnlySpan<byte> data)
    {
        if (_observer is null || data.Length == 0)
        {
            return;
        }

        try
        {
            _observer.OnChunk(new TrafficChunk(DateTimeOffset.UtcNow, direction, Description, data.ToArray()));
        }
        catch (Exception)
        {
            // Observers must not break the connection.
        }
    }

    public ValueTask DisposeAsync()
    {
        _socket.Dispose();
        return ValueTask.CompletedTask;
    }
}

internal sealed class StreamByteTransport : IByteTransport
{
    private readonly Stream _stream;

    private readonly ITrafficObserver? _observer;

    public StreamByteTransport(Stream stream, IPAddress? remoteAddress, string description, ITrafficObserver? observer = null, string? peerCertificateSubject = null, string? serverKeyId = null)
    {
        _stream = stream;
        RemoteAddress = remoteAddress;
        Description = description;
        _observer = observer;
        PeerCertificateSubject = peerCertificateSubject;
        ServerKeyId = serverKeyId;
    }

    public IPAddress? RemoteAddress { get; }

    public string Description { get; }

    public string? PeerCertificateSubject { get; }

    public string? ServerKeyId { get; }

    public async ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        await _stream.WriteAsync(data, cancellationToken).ConfigureAwait(false);
        await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        Observe(TrafficDirection.Outbound, data.Span);
    }

    public async ValueTask<int> ReceiveAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var count = await _stream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        if (count > 0)
        {
            Observe(TrafficDirection.Inbound, buffer.Span[..count]);
        }

        return count;
    }

    private void Observe(TrafficDirection direction, ReadOnlySpan<byte> data)
    {
        if (_observer is null || data.Length == 0)
        {
            return;
        }

        try
        {
            _observer.OnChunk(new TrafficChunk(DateTimeOffset.UtcNow, direction, Description, data.ToArray()));
        }
        catch (Exception)
        {
        }
    }

    public ValueTask DisposeAsync() => _stream.DisposeAsync();
}
