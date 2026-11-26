using System.Buffers;
using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;
using DomeMQ;

namespace DomeMQ.Client;

/// <summary>
/// Connected DomeMQ client. Application payloads are raw bytes. Use <see cref="IPayload"/> or
/// <see cref="IPayloadSerializer{T}"/> when the caller wants a serializer.
/// </summary>
public sealed class DomeMqClient : IAsyncDisposable
{
    private readonly DomeMqClientOptions _options;
    private readonly ConcurrentDictionary<uint, TaskCompletionSource<Frame>> _pending = new();
    private readonly ConcurrentDictionary<string, int> _channelLimits = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _write = new(1, 1);
    private readonly CancellationTokenSource _lifetime = new();
    private IByteTransport? _transport;
    private Task? _reader;
    private Task? _heartbeat;
    private int _serverMaxPayload;
    private int _wireCap;
    private uint _nextRequestId;

    private DomeMqClient(DomeMqClientOptions options) => _options = options;

    public string SessionId { get; private set; } = "";

    public int ServerMaxPayloadSize => _serverMaxPayload;

    public static async Task<DomeMqClient> ConnectAsync(DomeMqClientOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        var client = new DomeMqClient(options);
        try
        {
            await client.ConnectCoreAsync(cancellationToken).ConfigureAwait(false);
            return client;
        }
        catch
        {
            await client.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public async Task<ChannelInfo> CreateChannelAsync(
        string name,
        ChannelKind kind,
        CreateChannelOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var body = MessageCodec.EncodeCreateChannel(new CreateChannelBody(
            name,
            (byte)kind,
            (uint)(options?.MaxQueueLength ?? 0),
            (uint)(options?.MaxPayloadSize ?? 0),
            (byte)(options?.FullPolicy ?? FullPolicy.Inherit),
            (uint)(options?.MaxBytes ?? 0)));
        var frame = await RequestAsync(MessageType.CreateChannel, body, FrameFlags.None, cancellationToken).ConfigureAwait(false);
        var ack = MessageCodec.DecodeCreateChannelAck(frame.Payload.Span);
        _channelLimits[ack.Name] = (int)ack.MaxPayloadSize;
        return new ChannelInfo
        {
            Name = ack.Name,
            Kind = (ChannelKind)ack.ChannelType,
            MaxQueueLength = (int)ack.MaxQueueLength,
            MaxPayloadSize = (int)ack.MaxPayloadSize,
            FullPolicy = (FullPolicy)ack.FullPolicy,
            MaxBytes = ack.MaxBytes
        };
    }

    public async Task DeleteChannelAsync(string name, CancellationToken cancellationToken = default)
    {
        var body = MessageCodec.EncodeDeleteChannel(new DeleteChannelBody(name));
        var frame = await RequestAsync(MessageType.DeleteChannel, body, FrameFlags.None, cancellationToken).ConfigureAwait(false);
        _ = MessageCodec.DecodeDeleteChannelAck(frame.Payload.Span);
        _channelLimits.TryRemove(name, out _);
    }

    public Task<PushResult> PushAsync(
        string channel,
        byte[] body,
        PushOptions? options = null,
        CancellationToken cancellationToken = default) =>
        PushAsync(channel, (ReadOnlyMemory<byte>)body, options, cancellationToken);

    public async Task<PushResult> PushAsync(
        string channel,
        ReadOnlyMemory<byte> body,
        PushOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        EnsureFits(body.Length, options?.MaxPayloadSize, channel);
        var payload = MessageCodec.EncodePush(new PushBody(
            channel,
            WireLimit(options?.MaxPayloadSize),
            options?.ContentType ?? "",
            body.ToArray()));
        var flags = options?.NoAck == true ? FrameFlags.NoAck : FrameFlags.None;
        if (options?.NoAck == true)
        {
            await SendOnlyAsync(MessageType.Push, payload, flags, cancellationToken).ConfigureAwait(false);
            return new PushResult(false, 0);
        }

        var frame = await RequestAsync(MessageType.Push, payload, flags, cancellationToken).ConfigureAwait(false);
        var ack = MessageCodec.DecodePushAck(frame.Payload.Span);
        return new PushResult(ack.Status == PushStatus.Discarded, (int)ack.Depth);
    }

    public Task<PushResult> PushAsync<TPayload>(
        string channel,
        TPayload payload,
        PushOptions? options = null,
        CancellationToken cancellationToken = default)
        where TPayload : IPayload
    {
        ArgumentNullException.ThrowIfNull(payload);
        return PushAsync(channel, Materialize(payload), options, cancellationToken);
    }

    public Task<PushResult> PushAsync<T>(
        string channel,
        T value,
        IPayloadSerializer<T> serializer,
        PushOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var buffer = new ArrayBufferWriter<byte>(256);
        serializer.Serialize(value, buffer);
        return PushAsync(channel, buffer.WrittenMemory, options, cancellationToken);
    }

    public async Task<IReadOnlyList<DomeMqMessage>> PopAsync(
        string channel,
        PopOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var timeout = options?.Timeout ?? _options.PopTimeout;
        var milliseconds = timeout <= TimeSpan.Zero ? 0u : (uint)Math.Min(timeout.TotalMilliseconds, uint.MaxValue);
        var body = MessageCodec.EncodePop(new PopBody(
            channel,
            (uint)Math.Max(1, options?.MaxMessages ?? 1),
            milliseconds,
            WireLimit(options?.MaxPayloadSize)));
        var wait = timeout + TimeSpan.FromSeconds(5);
        if (wait < _options.RequestTimeout)
        {
            wait = _options.RequestTimeout;
        }

        var frame = await RequestAsync(MessageType.Pop, body, FrameFlags.None, wait, cancellationToken).ConfigureAwait(false);
        var ack = MessageCodec.DecodePopAck(frame.Payload.Span);
        var limit = EffectiveLimit(options?.MaxPayloadSize, channel);
        var messages = new List<DomeMqMessage>(ack.Messages.Length);
        foreach (var message in ack.Messages)
        {
            if (message.Body.Length > limit)
            {
                throw new PayloadTooLargeException(message.Body.Length, limit);
            }

            messages.Add(new DomeMqMessage(message.Body, message.ContentType, frame.RequestId));
        }

        return messages;
    }

    public async Task<IReadOnlyList<TPayload>> PopAsync<TPayload>(
        string channel,
        PopOptions? options = null,
        CancellationToken cancellationToken = default)
        where TPayload : IPayload, new()
    {
        var messages = await PopAsync(channel, options, cancellationToken).ConfigureAwait(false);
        var list = new List<TPayload>(messages.Count);
        foreach (var message in messages)
        {
            var payload = new TPayload();
            payload.SetPayload(message.Body);
            list.Add(payload);
        }

        return list;
    }

    public async Task<IReadOnlyList<T>> PopAsync<T>(
        string channel,
        IPayloadSerializer<T> serializer,
        PopOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        var messages = await PopAsync(channel, options, cancellationToken).ConfigureAwait(false);
        var list = new List<T>(messages.Count);
        foreach (var message in messages)
        {
            list.Add(serializer.Deserialize(message.Body));
        }

        return list;
    }

    public async Task PingAsync(CancellationToken cancellationToken = default)
    {
        var body = MessageCodec.EncodePing(new PingBody(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));
        var frame = await RequestAsync(MessageType.Ping, body, FrameFlags.None, cancellationToken).ConfigureAwait(false);
        _ = MessageCodec.DecodePong(frame.Payload.Span);
    }

    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        var body = MessageCodec.EncodeDisconnect(new DisconnectBody((ushort)DisconnectReason.Normal, "client disconnect"));
        await SendOnlyAsync(MessageType.Disconnect, body, FrameFlags.None, cancellationToken).ConfigureAwait(false);
        _lifetime.Cancel();
    }

    public async ValueTask DisposeAsync()
    {
        _lifetime.Cancel();
        if (_transport is not null)
        {
            await _transport.DisposeAsync().ConfigureAwait(false);
        }

        if (_reader is not null)
        {
            try
            {
                await _reader.ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }

        if (_heartbeat is not null)
        {
            try
            {
                await _heartbeat.ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }

        Fault(new DomeMqException("The client is closed."));
        _write.Dispose();
        _lifetime.Dispose();
    }

    private async Task ConnectCoreAsync(CancellationToken cancellationToken)
    {
        var settings = new TransportSettings
        {
            Transport = _options.Transport,
            Host = _options.Host,
            Port = _options.Port,
            Path = _options.Path,
            Tls = BuildTls(),
            TrafficObserver = _options.TrafficObserver
        };
        var factory = TransportRegistry.Resolve(_options.Transport);
        _transport = await factory.ConnectAsync(settings, cancellationToken).ConfigureAwait(false);
        _wireCap = ProtocolConstants.ControlFrameCap;
        _reader = Task.Run(ReadLoopAsync, CancellationToken.None);
        var connect = MessageCodec.EncodeConnect(new ConnectBody(
            _options.ClientName,
            ProtocolConstants.Version,
            _options.AuthToken ?? ""));
        var helloFrame = await RequestAsync(MessageType.Connect, connect, FrameFlags.None, cancellationToken).ConfigureAwait(false);
        var hello = MessageCodec.DecodeHello(helloFrame.Payload.Span);
        SessionId = hello.SessionId;
        _serverMaxPayload = (int)hello.MaxPayloadSize;
        _wireCap = PayloadLimits.WireCap(_serverMaxPayload);
        if (_options.HeartbeatInterval > TimeSpan.Zero)
        {
            _heartbeat = Task.Run(HeartbeatLoopAsync, CancellationToken.None);
        }
    }

    private TlsParameters? BuildTls()
    {
        if (_options.Tls is not { Enabled: true } tls)
        {
            return null;
        }

        X509Certificate2? certificate = null;
        if (!string.IsNullOrWhiteSpace(tls.PfxPath))
        {
            certificate = X509CertificateLoader.LoadPkcs12FromFile(tls.PfxPath, tls.PfxPassword);
        }
        else if (!string.IsNullOrWhiteSpace(tls.CertificatePath))
        {
            var loaded = string.IsNullOrWhiteSpace(tls.KeyPassword)
                ? X509Certificate2.CreateFromPemFile(tls.CertificatePath, tls.KeyPath)
                : X509Certificate2.CreateFromEncryptedPemFile(tls.CertificatePath, tls.KeyPassword, tls.KeyPath);
            certificate = CopyWithPersistedKey(loaded);
        }

        return new TlsParameters
        {
            Enabled = true,
            TargetHost = tls.TargetHost,
            AllowUntrustedCertificates = tls.AllowUntrustedCertificates,
            Protocols = tls.Protocols,
            ClientCertificate = certificate
        };
    }

    private static X509Certificate2 CopyWithPersistedKey(X509Certificate2 certificate)
    {
        try
        {
            var exported = certificate.Export(X509ContentType.Pkcs12);
            return X509CertificateLoader.LoadPkcs12(exported, password: null);
        }
        finally
        {
            certificate.Dispose();
        }
    }

    private Task<Frame> RequestAsync(MessageType type, byte[] payload, ushort flags, CancellationToken cancellationToken) =>
        RequestAsync(type, payload, flags, _options.RequestTimeout, cancellationToken);

    private async Task<Frame> RequestAsync(
        MessageType type,
        byte[] payload,
        ushort flags,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var id = NextId();
        var pending = new TaskCompletionSource<Frame>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[id] = pending;
        try
        {
            await SendAsync(FrameCodec.Create(type, id, payload, flags), cancellationToken).ConfigureAwait(false);
            var frame = await pending.Task.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
            ThrowIfError(frame);
            return frame;
        }
        catch
        {
            _pending.TryRemove(id, out _);
            throw;
        }
    }

    private async Task SendOnlyAsync(MessageType type, byte[] payload, ushort flags, CancellationToken cancellationToken)
    {
        var id = NextId();
        await SendAsync(FrameCodec.Create(type, id, payload, flags), cancellationToken).ConfigureAwait(false);
    }

    private async Task SendAsync(Frame frame, CancellationToken cancellationToken)
    {
        if (_transport is null)
        {
            throw new DomeMqException("The client is not connected.");
        }

        await _write.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await FrameIO.WriteAsync(_transport, frame, _options.WriteTimeout, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _write.Release();
        }
    }

    private async Task ReadLoopAsync()
    {
        try
        {
            while (!_lifetime.IsCancellationRequested && _transport is not null)
            {
                var frame = await FrameIO.ReadAsync(
                    _transport,
                    _wireCap,
                    Timeout.InfiniteTimeSpan,
                    _options.ReadTimeout,
                    _lifetime.Token).ConfigureAwait(false);
                if (frame.Type == (byte)MessageType.Ping)
                {
                    var ping = MessageCodec.DecodePing(frame.Payload.Span);
                    var pong = MessageCodec.EncodePong(new PongBody(ping.UnixTimeMilliseconds, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));
                    await SendAsync(FrameCodec.Create(MessageType.Pong, frame.RequestId, pong), _lifetime.Token).ConfigureAwait(false);
                    continue;
                }

                if (frame.Type == (byte)MessageType.Disconnect)
                {
                    Fault(new DomeMqException("The server disconnected."));
                    break;
                }

                if (_pending.TryRemove(frame.RequestId, out var pending))
                {
                    pending.TrySetResult(frame);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !_lifetime.IsCancellationRequested)
        {
            if (!_lifetime.IsCancellationRequested)
            {
                Fault(ex);
            }
        }
    }

    private async Task HeartbeatLoopAsync()
    {
        try
        {
            while (!_lifetime.IsCancellationRequested)
            {
                await Task.Delay(_options.HeartbeatInterval, _lifetime.Token).ConfigureAwait(false);
                await PingAsync(_lifetime.Token).ConfigureAwait(false);
            }
        }
        catch (Exception) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Fault(ex);
        }
    }

    private void EnsureFits(int size, int? messageLimit, string channel)
    {
        if (_serverMaxPayload <= 0)
        {
            return;
        }

        var limit = EffectiveLimit(messageLimit, channel);
        if (size > limit)
        {
            throw new PayloadTooLargeException(size, limit);
        }
    }

    private int EffectiveLimit(int? messageLimit, string channel)
    {
        int? channelLimit = _channelLimits.TryGetValue(channel, out var known) ? known : null;
        return PayloadLimits.Min(_serverMaxPayload, _options.MaxPayloadSize, channelLimit, messageLimit);
    }

    private uint WireLimit(int? messageLimit)
    {
        int? cap = _options.MaxPayloadSize;
        if (messageLimit is int message)
        {
            cap = cap is int current ? Math.Min(current, message) : message;
        }

        return cap is int value and > 0 ? (uint)value : 0;
    }

    private uint NextId()
    {
        var id = Interlocked.Increment(ref _nextRequestId);
        return id == 0 ? Interlocked.Increment(ref _nextRequestId) : id;
    }

    private static void ThrowIfError(Frame frame)
    {
        if (frame.Type != (byte)MessageType.Error)
        {
            return;
        }

        var error = MessageCodec.DecodeError(frame.Payload.Span);
        var code = Enum.IsDefined(typeof(ErrorCode), error.Code) ? (ErrorCode)error.Code : ErrorCode.Unspecified;
        if (code == ErrorCode.PayloadTooLarge)
        {
            throw new PayloadTooLargeException(error.Message);
        }

        throw new DomeMqException(error.Message, code);
    }

    private static ReadOnlyMemory<byte> Materialize(IPayload payload)
    {
        if (payload is IWritablePayload writable)
        {
            var buffer = new ArrayBufferWriter<byte>(writable.SizeHint ?? 256);
            writable.WriteTo(buffer);
            return buffer.WrittenMemory;
        }

        return payload.GetPayload();
    }

    private void Fault(Exception exception)
    {
        foreach (var pending in _pending)
        {
            if (_pending.TryRemove(pending.Key, out var source))
            {
                source.TrySetException(exception);
            }
        }
    }
}
