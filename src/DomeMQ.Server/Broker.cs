using System.Net;
using System.Net.Sockets;
using System.Text;
using DomeMQ;

namespace DomeMQ.Server;

public interface IBrokerConnectionObserver
{
    void OnConnected(BrokerConnectionSnapshot snapshot);

    void OnDisconnected(string id);
}

public sealed class BrokerConnectionSnapshot
{
    public required string Id { get; init; }

    public required string Remote { get; init; }

    public required DateTimeOffset ConnectedAt { get; init; }

    public string? ClientCertificateSubject { get; init; }
}

public sealed class ListenerInfo
{
    public required string Name { get; init; }

    public required string Transport { get; init; }

    public required string Description { get; init; }

    public EndPoint? LocalEndPoint { get; init; }
}

/// <summary>Embeddable DomeMQ broker. Reference this type from a generic host or from application code.</summary>
public sealed class DomeMqBroker : IAsyncDisposable
{
    private readonly DomeMqOptions _options;
    private readonly IBrokerLogger? _logger;
    private readonly BrokerEngine _engine;
    private readonly IpAccessDirectory _ip = new();
    private readonly KeyRing _keys = new();
    private readonly List<ITransportListener> _listeners = [];
    private readonly List<ListenerInfo> _listenerInfos = [];
    private readonly List<Task> _loops = [];
    private readonly CancellationTokenSource _stop = new();
    private HealthServer? _health;
    private int _clients;
    private int _state;

    public DomeMqBroker(DomeMqOptions options, IBrokerLogger? logger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger;
        _engine = new BrokerEngine(options, logger);
        _engine.Pressure += notice => QueuePressure?.Invoke(this, notice);
    }

    public event EventHandler<QueuePressure>? QueuePressure;

    /// <summary>Set before <see cref="StartAsync"/> to record every accepted connection.</summary>
    public IBrokerConnectionObserver? ConnectionObserver { get; set; }

    /// <summary>Set before <see cref="StartAsync"/>. Each accepted connection reports partial reads and writes.</summary>
    public ITrafficObserver? TrafficObserver { get; set; }

    public BrokerStatistics GetStatistics() => _engine.GetStatistics();

    public IReadOnlyList<ListenerInfo> Listeners => _listenerInfos;

    public EndPoint? HealthEndPoint => _health?.LocalEndPoint;

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref _state, 1, 0) != 0)
        {
            throw new InvalidOperationException("The broker has already been started.");
        }

        DomeMqOptionsValidator.ValidateAndThrow(_options);
        _keys.Load(_options);
        _ip.Replace(_options);
        foreach (var listener in _options.Listeners.Where(static item => item.Enabled))
        {
            var settings = BuildSettings(listener);
            var factory = TransportRegistry.Resolve(listener.Transport);
            var bound = factory.Listen(settings);
            _listeners.Add(bound);
            _listenerInfos.Add(new ListenerInfo
            {
                Name = listener.Name,
                Transport = TransportRegistry.Normalize(listener.Transport),
                Description = bound.Description,
                LocalEndPoint = bound.LocalEndPoint
            });
            _logger?.Log(BrokerLogLevel.Information, "Listener", $"Listening on {bound.Description} ({listener.Name}).");
            var captured = listener;
            _loops.Add(Task.Run(() => AcceptLoopAsync(captured, bound, _stop.Token), CancellationToken.None));
        }

        if (_listenerInfos.Count == 0)
        {
            _logger?.Log(BrokerLogLevel.Warning, "Listener", "No listeners are enabled. The broker will not accept connections.");
        }

        if (_options.Health.Enabled)
        {
            _health = new HealthServer(_options.Health);
            _health.Start();
            _logger?.Log(BrokerLogLevel.Information, "Health", $"Health endpoint listening on {_health.LocalEndPoint} {_options.Health.Path}.");
        }

        if (_options.Channels.CleanupInterval > TimeSpan.Zero)
        {
            _loops.Add(Task.Run(() => CleanupLoopAsync(_stop.Token), CancellationToken.None));
        }

        await Task.CompletedTask.ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref _state, 2, 1) != 1)
        {
            return;
        }

        _stop.Cancel();
        foreach (var listener in _listeners)
        {
            await listener.DisposeAsync().ConfigureAwait(false);
        }

        if (_health is not null)
        {
            await _health.DisposeAsync().ConfigureAwait(false);
        }

        try
        {
            await Task.WhenAll(_loops).WaitAsync(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is TimeoutException or OperationCanceledException)
        {
        }

        _keys.Dispose();
    }

    public void UpdateIpAccess(DomeMqOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        DomeMqOptionsValidator.Normalize(options);
        _ip.Replace(options);
        _logger?.Log(BrokerLogLevel.Information, "IpAccess", "Reloaded IP access rules.");
    }

    /// <summary>
    /// Reloads IP access rules and TLS key material. New connections use the new certificates.
    /// Existing TLS sessions keep the certificate they already negotiated.
    /// Enabling TLS on a listener that was started without it still requires a restart.
    /// </summary>
    public void ReloadHotConfiguration(DomeMqOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        DomeMqOptionsValidator.Normalize(options);
        var changed = _keys.Load(options);
        _ip.Replace(options);
        if (changed)
        {
            _logger?.Log(BrokerLogLevel.Information, "Tls", "Reloaded TLS key material for new connections.");
        }
    }

    public bool IsIpAllowed(string listenerName, IPAddress address) => _ip.IsAllowed(listenerName, address);

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _stop.Dispose();
    }

    private TransportSettings BuildSettings(ListenerOptions listener)
    {
        var tcp = _options.Tcp;
        TlsParameters? tls = null;
        if (TransportRegistry.Normalize(listener.Transport) == "tcp")
        {
            var tlsOptions = DomeMqOptionsValidator.EffectiveTls(_options, listener);
            if (tlsOptions.Enabled)
            {
                var listenerName = listener.Name;
                tls = new TlsParameters
                {
                    Enabled = true,
                    HandshakeSelector = () => _keys.Require(listenerName)
                };
            }
        }

        return new TransportSettings
        {
            Transport = listener.Transport,
            Host = listener.Host,
            Port = listener.Port,
            Path = listener.Path,
            NoDelay = listener.NoDelay ?? tcp.NoDelay,
            KeepAlive = listener.KeepAlive ?? tcp.KeepAlive,
            KeepAliveTimeSeconds = listener.KeepAliveTimeSeconds ?? tcp.KeepAliveTimeSeconds,
            KeepAliveIntervalSeconds = listener.KeepAliveIntervalSeconds ?? tcp.KeepAliveIntervalSeconds,
            ReceiveBufferSize = listener.ReceiveBufferSize ?? tcp.ReceiveBufferSize,
            SendBufferSize = listener.SendBufferSize ?? tcp.SendBufferSize,
            Backlog = listener.Backlog ?? tcp.Backlog,
            LingerSeconds = listener.LingerSeconds ?? tcp.LingerSeconds,
            DeleteStaleSocket = listener.DeleteStaleSocket ?? _options.Unix.DeleteStaleSocket,
            TrafficObserver = TrafficObserver,
            UnixPermissions = DomeMqOptionsValidator.ParseUnixMode(listener.Permissions ?? _options.Unix.Permissions),
            Tls = tls
        };
    }

    private async Task AcceptLoopAsync(ListenerOptions listener, ITransportListener bound, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            IByteTransport? transport = null;
            try
            {
                transport = await bound.AcceptAsync(cancellationToken).ConfigureAwait(false);
                if (TransportRegistry.AppliesIpAccess(listener.Transport)
                    && transport.RemoteAddress is IPAddress remote
                    && !_ip.IsAllowed(listener.Name, remote))
                {
                    _logger?.Log(BrokerLogLevel.Warning, "IpAccess", $"Rejected {remote} on listener '{listener.Name}' before handshake.");
                    await transport.DisposeAsync().ConfigureAwait(false);
                    continue;
                }

                if (Interlocked.Increment(ref _clients) > _options.Limits.MaxClients)
                {
                    Interlocked.Decrement(ref _clients);
                    _logger?.Log(BrokerLogLevel.Warning, "Limits", $"Rejected a connection on '{listener.Name}' because MaxClients has been reached.");
                    await transport.DisposeAsync().ConfigureAwait(false);
                    continue;
                }

                var session = new Session
                {
                    ConnectionPayloadLimit = PayloadLimits.Min(_options.Limits.MaxPayloadSize, listener.MaxPayloadSize, null, null),
                    ListenerName = listener.Name,
                    Remote = transport.Description
                };
                var accepted = transport;
                transport = null;
                _ = Task.Run(() => RunConnectionAsync(accepted, session), CancellationToken.None);
            }
            catch (Exception ex) when (cancellationToken.IsCancellationRequested || ex is ObjectDisposedException or SocketException)
            {
                if (transport is not null)
                {
                    await transport.DisposeAsync().ConfigureAwait(false);
                }

                break;
            }
            catch (Exception ex)
            {
                _logger?.Log(BrokerLogLevel.Error, "Accept", $"Accept failed on {bound.Description}.", ex);
                if (transport is not null)
                {
                    await transport.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
    }

    private async Task RunConnectionAsync(IByteTransport transport, Session session)
    {
        var writer = new FrameWriter(transport, _options.Timeouts.WriteTimeout);
        using var connectionStop = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        var connectedAt = DateTimeOffset.UtcNow;
        try
        {
            try
            {
                ConnectionObserver?.OnConnected(new BrokerConnectionSnapshot
                {
                    Id = session.PublicId,
                    Remote = transport.Description,
                    ConnectedAt = connectedAt,
                    ClientCertificateSubject = transport.PeerCertificateSubject
                });
            }
            catch (Exception)
            {
            }
            var cap = new Func<FrameHeader, int>(header => PayloadLimits.InboundFrameCap(header.Type, session.ConnectionPayloadLimit));
            Frame frame;
            try
            {
                frame = await FrameIO.ReadAsync(
                    transport,
                    cap,
                    _options.Timeouts.HandshakeTimeout,
                    _options.Timeouts.ReadTimeout,
                    connectionStop.Token).ConfigureAwait(false);
            }
            catch (PayloadTooLargeException ex)
            {
                await RejectOversizeAsync(writer, session, ex).ConfigureAwait(false);
                return;
            }
            catch (TimeoutException)
            {
                _logger?.Log(BrokerLogLevel.Warning, "Timeout", $"Handshake timed out for {session.Remote}.");
                await TryDisconnectAsync(writer, DisconnectReason.HandshakeTimeout, "Handshake timed out.").ConfigureAwait(false);
                return;
            }

            var result = await _engine.DispatchAsync(session, frame, connectionStop.Token).ConfigureAwait(false);
            if (result.Response is Frame response)
            {
                await writer.WriteAsync(response, connectionStop.Token).ConfigureAwait(false);
            }

            if (result.Close || !session.HandshakeComplete)
            {
                return;
            }

            var heartbeat = HeartbeatAsync(writer, connectionStop);
            try
            {
                while (!connectionStop.IsCancellationRequested)
                {
                    try
                    {
                        frame = await FrameIO.ReadAsync(
                            transport,
                            cap,
                            _options.Timeouts.IdleTimeout,
                            _options.Timeouts.ReadTimeout,
                            connectionStop.Token).ConfigureAwait(false);
                    }
                    catch (PayloadTooLargeException ex)
                    {
                        await RejectOversizeAsync(writer, session, ex).ConfigureAwait(false);
                        break;
                    }
                    catch (TimeoutException)
                    {
                        _logger?.Log(BrokerLogLevel.Information, "Timeout", $"Idle timeout for {session.Remote}.");
                        await TryDisconnectAsync(writer, DisconnectReason.IdleTimeout, "Idle timeout.").ConfigureAwait(false);
                        break;
                    }

                    result = await _engine.DispatchAsync(session, frame, connectionStop.Token).ConfigureAwait(false);
                    if (result.Response is Frame next)
                    {
                        await writer.WriteAsync(next, connectionStop.Token).ConfigureAwait(false);
                        if (next.TryGetMessageType(out var messageType) && messageType == MessageType.Error)
                        {
                            var error = MessageCodec.DecodeError(next.Payload.Span);
                            if (error.Code == (ushort)ErrorCode.PayloadTooLarge)
                            {
                                _logger?.Log(BrokerLogLevel.Warning, "Payload", $"Rejected an oversized payload from {session.Remote}: {error.Message}");
                            }
                        }
                    }

                    if (result.Close)
                    {
                        break;
                    }
                }
            }
            finally
            {
                connectionStop.Cancel();
                try
                {
                    await heartbeat.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
            await TryDisconnectAsync(writer, DisconnectReason.Shutdown, "Broker is shutting down.").ConfigureAwait(false);
        }
        catch (ProtocolException ex)
        {
            _logger?.Log(BrokerLogLevel.Warning, "Protocol", $"Closing {session.Remote}: {ex.Message}");
        }
        catch (Exception ex) when (ex is IOException or SocketException or EndOfStreamException)
        {
            _logger?.Log(BrokerLogLevel.Debug, "Connection", $"Connection closed ({session.Remote}).", ex);
        }
        catch (Exception ex)
        {
            _logger?.Log(BrokerLogLevel.Error, "Connection", $"Connection failed ({session.Remote}).", ex);
        }
        finally
        {
            try
            {
                ConnectionObserver?.OnDisconnected(session.PublicId);
            }
            catch (Exception)
            {
            }

            _engine.DropSession(session);
            Interlocked.Decrement(ref _clients);
            await transport.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task HeartbeatAsync(FrameWriter writer, CancellationTokenSource stop)
    {
        var interval = _options.Timeouts.HeartbeatInterval;
        if (interval <= TimeSpan.Zero)
        {
            return;
        }

        var requestId = 0u;
        try
        {
            while (!stop.IsCancellationRequested)
            {
                await Task.Delay(interval, stop.Token).ConfigureAwait(false);
                requestId = requestId == uint.MaxValue ? 1 : requestId + 1;
                var payload = MessageCodec.EncodePing(new PingBody(_engine.Clock().ToUnixTimeMilliseconds()));
                await writer.WriteAsync(FrameCodec.Create(MessageType.Ping, requestId, payload), stop.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            _logger?.Log(BrokerLogLevel.Debug, "Heartbeat", "Heartbeat failed.", ex);
            stop.Cancel();
        }
    }

    private async Task RejectOversizeAsync(FrameWriter writer, Session session, PayloadTooLargeException exception)
    {
        _logger?.Log(BrokerLogLevel.Warning, "Payload", $"Rejected an oversize frame from {session.Remote}: {exception.Message}");
        var payload = MessageCodec.EncodeError(new ErrorBody((ushort)ErrorCode.PayloadTooLarge, Trim(exception.Message), 0));
        await TryWriteAsync(writer, FrameCodec.Create(MessageType.Error, 0, payload)).ConfigureAwait(false);
        await TryDisconnectAsync(writer, DisconnectReason.PayloadTooLarge, "Payload too large.").ConfigureAwait(false);
    }

    private static async Task TryDisconnectAsync(FrameWriter writer, DisconnectReason reason, string text)
    {
        var payload = MessageCodec.EncodeDisconnect(new DisconnectBody((ushort)reason, text));
        await TryWriteAsync(writer, FrameCodec.Create(MessageType.Disconnect, 0, payload)).ConfigureAwait(false);
    }

    private static async Task TryWriteAsync(FrameWriter writer, Frame frame)
    {
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            await writer.WriteAsync(frame, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception)
        {
        }
    }

    private async Task CleanupLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.Channels.CleanupInterval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            _engine.Cleanup();
        }
    }

    private static string Trim(string message)
    {
        if (Encoding.UTF8.GetByteCount(message) <= ProtocolConstants.MaxErrorMessageBytes)
        {
            return message;
        }

        var span = message.AsSpan();
        while (span.Length > 0 && Encoding.UTF8.GetByteCount(span) > ProtocolConstants.MaxErrorMessageBytes)
        {
            span = span[..^1];
        }

        return span.ToString();
    }

    private sealed class FrameWriter(IByteTransport transport, TimeSpan timeout)
    {
        private readonly SemaphoreSlim _gate = new(1, 1);

        public async ValueTask WriteAsync(Frame frame, CancellationToken cancellationToken)
        {
            await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await FrameIO.WriteAsync(transport, frame, timeout, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _gate.Release();
            }
        }
    }

    private sealed class HealthServer : IAsyncDisposable
    {
        private readonly HealthOptions _options;
        private readonly Socket _socket;
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _loop;

        public HealthServer(HealthOptions options)
        {
            _options = options;
            var address = IPAddress.Parse(options.Host);
            _socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _socket.Bind(new IPEndPoint(address, options.Port));
            _socket.Listen(16);
            LocalEndPoint = _socket.LocalEndPoint;
            _loop = Task.Run(AcceptAsync);
        }

        public EndPoint? LocalEndPoint { get; }

        public void Start()
        {
        }

        private async Task AcceptAsync()
        {
            while (!_stop.IsCancellationRequested)
            {
                Socket? client = null;
                try
                {
                    client = await _socket.AcceptAsync(_stop.Token).ConfigureAwait(false);
                    var captured = client;
                    client = null;
                    _ = Task.Run(() => ServeAsync(captured));
                }
                catch (Exception) when (_stop.IsCancellationRequested)
                {
                    client?.Dispose();
                    break;
                }
                catch (Exception)
                {
                    client?.Dispose();
                }
            }
        }

        private async Task ServeAsync(Socket client)
        {
            using (client)
            {
                var buffer = new byte[2048];
                var read = 0;
                try
                {
                    while (read < buffer.Length)
                    {
                        var count = await client.ReceiveAsync(buffer.AsMemory(read), SocketFlags.None).ConfigureAwait(false);
                        if (count == 0)
                        {
                            break;
                        }

                        read += count;
                        if (Encoding.ASCII.GetString(buffer, 0, read).Contains("\r\n\r\n", StringComparison.Ordinal))
                        {
                            break;
                        }
                    }

                    var request = Encoding.ASCII.GetString(buffer, 0, read);
                    var path = "/";
                    var first = request.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
                    if (first.Length >= 2)
                    {
                        path = first[1];
                        var query = path.IndexOf('?');
                        if (query >= 0)
                        {
                            path = path[..query];
                        }
                    }

                    var ok = request.StartsWith("GET ", StringComparison.Ordinal) && path == _options.Path;
                    var body = ok ? "{\"status\":\"ok\"}" : "{\"status\":\"not_found\"}";
                    var status = ok ? "200 OK" : "404 Not Found";
                    var response = $"HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {body.Length}\r\nConnection: close\r\n\r\n{body}";
                    var bytes = Encoding.ASCII.GetBytes(response);
                    await client.SendAsync(bytes, SocketFlags.None).ConfigureAwait(false);
                }
                catch (Exception)
                {
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();
            _socket.Dispose();
            try
            {
                await _loop.ConfigureAwait(false);
            }
            catch (Exception)
            {
            }

            _stop.Dispose();
        }
    }
}
