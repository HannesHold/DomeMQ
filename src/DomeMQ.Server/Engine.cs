using System.Security.Cryptography;
using System.Text;
using DomeMQ;

namespace DomeMQ.Server;

internal sealed class Session
{
    public Guid Id { get; } = Guid.NewGuid();

    public string PublicId { get; } = Guid.NewGuid().ToString("N");

    public bool HandshakeComplete { get; set; }

    public int ConnectionPayloadLimit { get; init; }

    public string ListenerName { get; init; } = "";

    public string Remote { get; init; } = "";

    public HashSet<string> OwnedChannels { get; } = new(StringComparer.Ordinal);
}

internal readonly struct EngineResult
{
    public Frame? Response { get; init; }

    public bool Close { get; init; }
}

internal sealed class BrokerEngine
{
    private readonly DomeMqOptions _options;
    private readonly bool _dropOldest;
    private readonly ChannelKind _defaultKind;
    private readonly int _globalPayload;
    private readonly int _defaultQueue;
    private readonly TimeSpan? _messageTtl;
    private readonly TimeSpan _orphanTtl;
    private readonly byte[][] _tokenHashes;
    private readonly bool _authRequired;
    private readonly object _gate = new();
    private readonly Dictionary<string, BrokerChannel> _channels = new(StringComparer.Ordinal);

    public BrokerEngine(DomeMqOptions options)
    {
        _options = options;
        DomeMqOptionsValidator.TryParseFullBehavior(options.Limits.FullBehavior, out _dropOldest);
        DomeMqOptionsValidator.TryParseChannelKind(options.Channels.DefaultType, out _defaultKind);
        _globalPayload = options.Limits.MaxPayloadSize;
        _defaultQueue = options.Limits.MaxQueueLengthPerChannel;
        _messageTtl = options.Channels.DefaultMessageTtl is TimeSpan ttl && ttl > TimeSpan.Zero ? ttl : null;
        _orphanTtl = options.Channels.OrphanChannelTtl;
        _authRequired = options.Authentication.Mode.Equals("SharedKey", StringComparison.OrdinalIgnoreCase);
        _tokenHashes = options.Authentication.Tokens.Select(token => SHA256.HashData(Encoding.UTF8.GetBytes(token))).ToArray();
    }

    public Func<DateTimeOffset> Clock { get; set; } = static () => DateTimeOffset.UtcNow;

    public async ValueTask<EngineResult> DispatchAsync(Session session, Frame frame, CancellationToken cancellationToken)
    {
        if (frame.Version != ProtocolConstants.Version)
        {
            return Error(frame, ErrorCode.UnsupportedVersion, "Unsupported protocol version.");
        }

        if (FrameFlags.HasUnknownMustUnderstand(frame.Flags))
        {
            return Error(frame, ErrorCode.ProtocolError, "Unknown must-understand flags.");
        }

        if (!frame.TryGetMessageType(out var type))
        {
            return Error(frame, ErrorCode.InvalidMessage, "Unknown message type.");
        }

        if (!session.HandshakeComplete && type != MessageType.Connect)
        {
            return Error(frame, ErrorCode.NotConnected, "Connect is required before any other message.", close: true);
        }

        try
        {
            switch (type)
            {
                case MessageType.Connect:
                    return HandleConnect(session, frame);
                case MessageType.CreateChannel:
                    return HandleCreate(session, frame);
                case MessageType.DeleteChannel:
                    return HandleDelete(session, frame);
                case MessageType.Push:
                    return HandlePush(session, frame);
                case MessageType.Pop:
                    return await HandlePopAsync(session, frame, cancellationToken).ConfigureAwait(false);
                case MessageType.Ping:
                    return HandlePing(frame);
                case MessageType.Pong:
                    return default;
                case MessageType.Disconnect:
                    return new EngineResult { Close = true };
                default:
                    return Error(frame, ErrorCode.InvalidMessage, "Unexpected message type.");
            }
        }
        catch (ProtocolException ex)
        {
            return Error(frame, ErrorCode.InvalidMessage, ex.Message);
        }
    }

    public void DropSession(Session session)
    {
        lock (_gate)
        {
            if (_orphanTtl <= TimeSpan.Zero)
            {
                foreach (var name in session.OwnedChannels)
                {
                    if (_channels.TryGetValue(name, out var channel))
                    {
                        channel.MarkRemoved();
                        _channels.Remove(name);
                    }
                }
            }
            else
            {
                var deadline = Clock() + _orphanTtl;
                foreach (var name in session.OwnedChannels)
                {
                    if (_channels.TryGetValue(name, out var channel))
                    {
                        channel.Orphan(deadline);
                    }
                }
            }

            session.OwnedChannels.Clear();
        }
    }

    public void Cleanup() => Cleanup(Clock());

    public void Cleanup(DateTimeOffset now)
    {
        lock (_gate)
        {
            List<string>? remove = null;
            foreach (var (name, channel) in _channels)
            {
                channel.DropExpired(now, _messageTtl);
                if (channel.OrphanDeadline is DateTimeOffset deadline && deadline <= now)
                {
                    remove ??= [];
                    remove.Add(name);
                }
            }

            if (remove is null)
            {
                return;
            }

            foreach (var name in remove)
            {
                if (_channels.TryGetValue(name, out var channel))
                {
                    channel.MarkRemoved();
                    _channels.Remove(name);
                }
            }
        }
    }

    private EngineResult HandleConnect(Session session, Frame frame)
    {
        if (session.HandshakeComplete)
        {
            return Error(frame, ErrorCode.ProtocolError, "Already connected.");
        }

        var body = MessageCodec.DecodeConnect(frame.Payload.Span);
        if (body.ProtocolVersion != frame.Version)
        {
            return Error(frame, ErrorCode.ProtocolError, "Header version and Connect version do not match.");
        }

        if (!Authorize(body.AuthToken))
        {
            return Error(frame, ErrorCode.Unauthorized, "Authentication failed.", close: true);
        }

        session.HandshakeComplete = true;
        var hello = MessageCodec.EncodeHello(new HelloBody(
            _options.ServerName,
            ProtocolConstants.Version,
            session.PublicId,
            (uint)session.ConnectionPayloadLimit,
            (uint)_defaultQueue,
            (byte)_defaultKind));
        return Reply(frame, MessageType.Hello, hello);
    }

    private EngineResult HandleCreate(Session session, Frame frame)
    {
        var body = MessageCodec.DecodeCreateChannel(frame.Payload.Span);
        if (!IsChannelName(body.Name))
        {
            return Error(frame, ErrorCode.InvalidMessage, "Channel name is empty or contains a control character.");
        }

        ChannelKind kind;
        if (body.ChannelType == 0)
        {
            kind = _defaultKind;
        }
        else if (!Enum.IsDefined(typeof(ChannelKind), body.ChannelType))
        {
            return Error(frame, ErrorCode.InvalidChannelType, "Unknown channel type.");
        }
        else
        {
            kind = (ChannelKind)body.ChannelType;
        }

        if (body.MaxQueueLength > (uint)_defaultQueue)
        {
            return Error(frame, ErrorCode.InvalidQueueLimit, "Requested queue length exceeds the server maximum.");
        }

        var queueLength = body.MaxQueueLength == 0 ? _defaultQueue : (int)body.MaxQueueLength;
        int? channelPayload = null;
        if (body.MaxPayloadSize != 0)
        {
            if (body.MaxPayloadSize > (uint)_globalPayload)
            {
                return Error(frame, ErrorCode.InvalidPayloadLimit, "Requested MaxPayloadSize exceeds the server global limit.");
            }

            channelPayload = (int)body.MaxPayloadSize;
        }

        lock (_gate)
        {
            if (_channels.ContainsKey(body.Name))
            {
                return Error(frame, ErrorCode.ChannelExists, "A channel with that name already exists.");
            }

            if (_channels.Count >= _options.Limits.MaxChannels)
            {
                return Error(frame, ErrorCode.TooManyChannels, "The server channel limit has been reached.");
            }

            if (session.OwnedChannels.Count >= _options.Limits.MaxChannelsPerClient)
            {
                return Error(frame, ErrorCode.TooManyChannels, "The per-client channel limit has been reached.");
            }

            var channel = new BrokerChannel(body.Name, kind, queueLength, channelPayload, session.Id);
            _channels.Add(body.Name, channel);
            session.OwnedChannels.Add(body.Name);
        }

        var ack = MessageCodec.EncodeCreateChannelAck(new CreateChannelAck(
            body.Name,
            (byte)kind,
            (uint)queueLength,
            (uint)(channelPayload ?? _globalPayload)));
        return Reply(frame, MessageType.Ack, ack);
    }

    private EngineResult HandleDelete(Session session, Frame frame)
    {
        var body = MessageCodec.DecodeDeleteChannel(frame.Payload.Span);
        lock (_gate)
        {
            if (!_channels.TryGetValue(body.Name, out var channel))
            {
                return Error(frame, ErrorCode.ChannelNotFound, "Channel was not found.");
            }

            if (channel.OwnerId != session.Id)
            {
                return Error(frame, ErrorCode.NotChannelOwner, "Only the channel owner can delete it.");
            }

            channel.MarkRemoved();
            _channels.Remove(body.Name);
            session.OwnedChannels.Remove(body.Name);
        }

        var ack = MessageCodec.EncodeDeleteChannelAck(new DeleteChannelAck(body.Name));
        return Reply(frame, MessageType.Ack, ack);
    }

    private EngineResult HandlePush(Session session, Frame frame)
    {
        var body = MessageCodec.DecodePush(frame.Payload.Span);
        BrokerChannel? channel;
        lock (_gate)
        {
            _channels.TryGetValue(body.ChannelName, out channel);
        }

        if (channel is null || channel.Removed)
        {
            return Error(frame, ErrorCode.ChannelNotFound, "Channel was not found.");
        }

        var limit = PayloadLimits.Min(
            session.ConnectionPayloadLimit,
            channel.MaxPayloadSize,
            OptionalLimit(body.MaxPayloadSize));
        if (body.Body.Length > limit)
        {
            return Error(frame, ErrorCode.PayloadTooLarge, $"Payload of {body.Body.Length} bytes exceeds the limit of {limit} bytes.");
        }

        var accepted = channel.TryPush(
            new StoredMessage(body.Body, body.ContentType, Clock()),
            _dropOldest,
            Clock(),
            _messageTtl,
            out var depth);
        if (!accepted)
        {
            if (channel.Removed)
            {
                return Error(frame, ErrorCode.ChannelNotFound, "Channel was not found.");
            }

            return Error(frame, ErrorCode.ChannelFull, "Channel is full.");
        }

        if ((frame.Flags & FrameFlags.NoAck) != 0)
        {
            return default;
        }

        var ack = MessageCodec.EncodePushAck(new PushAck(body.ChannelName, (uint)depth));
        return Reply(frame, MessageType.Ack, ack);
    }

    private async ValueTask<EngineResult> HandlePopAsync(Session session, Frame frame, CancellationToken cancellationToken)
    {
        var body = MessageCodec.DecodePop(frame.Payload.Span);
        if (body.MaxMessages is 0 or > ProtocolConstants.MaxPopBatch)
        {
            return Error(frame, ErrorCode.InvalidMessage, $"maxMessages must be between 1 and {ProtocolConstants.MaxPopBatch}.");
        }

        BrokerChannel? channel;
        lock (_gate)
        {
            _channels.TryGetValue(body.ChannelName, out channel);
        }

        if (channel is null || channel.Removed)
        {
            return Error(frame, ErrorCode.ChannelNotFound, "Channel was not found.");
        }

        if (channel.OwnerId != session.Id)
        {
            return Error(frame, ErrorCode.NotChannelOwner, "Only the channel owner can pop it.");
        }

        var limit = PayloadLimits.Min(
            session.ConnectionPayloadLimit,
            channel.MaxPayloadSize,
            OptionalLimit(body.MaxPayloadSize));
        var timeout = body.TimeoutMilliseconds == 0
            ? TimeSpan.Zero
            : TimeSpan.FromMilliseconds(Math.Min(body.TimeoutMilliseconds, int.MaxValue));
        var outcome = await channel.PopAsync(
            (int)body.MaxMessages,
            timeout,
            limit,
            PayloadLimits.WireCap(session.ConnectionPayloadLimit),
            Clock,
            _messageTtl,
            cancellationToken).ConfigureAwait(false);
        if (outcome.TooLarge)
        {
            return Error(frame, ErrorCode.PayloadTooLarge, $"The next message exceeds the pop limit of {limit} bytes.");
        }

        var deliveries = outcome.Messages.Select(message => new Delivery(message.ContentType, message.Body)).ToArray();
        var ack = MessageCodec.EncodePopAck(new PopAck(body.ChannelName, deliveries));
        return Reply(frame, MessageType.Ack, ack);
    }

    private EngineResult HandlePing(Frame frame)
    {
        var body = MessageCodec.DecodePing(frame.Payload.Span);
        var pong = MessageCodec.EncodePong(new PongBody(body.UnixTimeMilliseconds, Clock().ToUnixTimeMilliseconds()));
        return Reply(frame, MessageType.Pong, pong);
    }

    private bool Authorize(string presented)
    {
        if (!_authRequired)
        {
            return true;
        }

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(presented));
        var match = false;
        foreach (var token in _tokenHashes)
        {
            if (CryptographicOperations.FixedTimeEquals(hash, token))
            {
                match = true;
            }
        }

        return match;
    }

    private static int? OptionalLimit(uint value) => value == 0 ? null : (int)Math.Min(value, int.MaxValue);

    private static bool IsChannelName(string name)
    {
        if (string.IsNullOrEmpty(name) || Encoding.UTF8.GetByteCount(name) > ProtocolConstants.MaxChannelNameBytes)
        {
            return false;
        }

        foreach (var character in name)
        {
            if (character is '\0' or '\r' or '\n')
            {
                return false;
            }
        }

        return true;
    }

    private static EngineResult Reply(Frame request, MessageType type, byte[] payload) =>
        new()
        {
            Response = FrameCodec.Create(type, request.RequestId, payload)
        };

    private static EngineResult Error(Frame request, ErrorCode code, string message, bool close = false)
    {
        var text = message;
        if (Encoding.UTF8.GetByteCount(text) > ProtocolConstants.MaxErrorMessageBytes)
        {
            text = TrimUtf8(text, ProtocolConstants.MaxErrorMessageBytes);
        }

        var payload = MessageCodec.EncodeError(new ErrorBody((ushort)code, text, request.Type));
        return new EngineResult
        {
            Response = FrameCodec.Create(MessageType.Error, request.RequestId, payload),
            Close = close
        };
    }

    private static string TrimUtf8(string value, int maxBytes)
    {
        var span = value.AsSpan();
        while (span.Length > 0 && Encoding.UTF8.GetByteCount(span) > maxBytes)
        {
            span = span[..^1];
        }

        return span.ToString();
    }
}

internal sealed class StoredMessage(byte[] body, string contentType, DateTimeOffset enqueuedAt)
{
    public byte[] Body { get; } = body;

    public string ContentType { get; } = contentType;

    public DateTimeOffset EnqueuedAt { get; } = enqueuedAt;
}

internal readonly struct PopOutcome(bool tooLarge, List<StoredMessage> messages)
{
    public bool TooLarge { get; } = tooLarge;

    public List<StoredMessage> Messages { get; } = messages;
}

internal sealed class BrokerChannel
{
    private readonly object _gate = new();
    private readonly LinkedList<StoredMessage> _items = new();
    private TaskCompletionSource<bool>? _waiter;

    public BrokerChannel(string name, ChannelKind kind, int maxLength, int? maxPayloadSize, Guid ownerId)
    {
        Name = name;
        Kind = kind;
        MaxLength = maxLength;
        MaxPayloadSize = maxPayloadSize;
        OwnerId = ownerId;
    }

    public string Name { get; }

    public ChannelKind Kind { get; }

    public int MaxLength { get; }

    public int? MaxPayloadSize { get; }

    public Guid OwnerId { get; }

    private bool _removed;
    private DateTimeOffset? _orphanDeadline;

    public bool Removed
    {
        get
        {
            lock (_gate)
            {
                return _removed;
            }
        }
    }

    public DateTimeOffset? OrphanDeadline
    {
        get
        {
            lock (_gate)
            {
                return _orphanDeadline;
            }
        }
    }

    public void MarkRemoved()
    {
        lock (_gate)
        {
            _removed = true;
            _waiter?.TrySetResult(false);
            _waiter = null;
        }
    }

    public void Orphan(DateTimeOffset deadline)
    {
        lock (_gate)
        {
            _orphanDeadline = deadline;
        }
    }

    public void DropExpired(DateTimeOffset now, TimeSpan? ttl)
    {
        lock (_gate)
        {
            DropExpiredLocked(now, ttl);
        }
    }

    public bool TryPush(StoredMessage message, bool dropOldest, DateTimeOffset now, TimeSpan? ttl, out int depth)
    {
        TaskCompletionSource<bool>? wake;
        lock (_gate)
        {
            if (Removed)
            {
                depth = 0;
                return false;
            }

            DropExpiredLocked(now, ttl);
            if (_items.Count >= MaxLength)
            {
                if (!dropOldest || _items.First is null)
                {
                    depth = _items.Count;
                    return false;
                }

                _items.RemoveFirst();
            }

            _items.AddLast(message);
            depth = _items.Count;
            wake = _waiter;
            _waiter = null;
        }

        wake?.TrySetResult(true);
        return true;
    }

    public async ValueTask<PopOutcome> PopAsync(
        int maxMessages,
        TimeSpan timeout,
        int payloadLimit,
        int frameBudget,
        Func<DateTimeOffset> clock,
        TimeSpan? ttl,
        CancellationToken cancellationToken)
    {
        TaskCompletionSource<bool>? waiter = null;
        lock (_gate)
        {
            var immediate = TakeLocked(maxMessages, payloadLimit, frameBudget, clock(), ttl);
            if (immediate.TooLarge || immediate.Messages.Count > 0 || timeout <= TimeSpan.Zero || Removed)
            {
                return immediate;
            }

            waiter = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiter = waiter;
        }

        try
        {
            await waiter.Task.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
        }
        finally
        {
            lock (_gate)
            {
                if (ReferenceEquals(_waiter, waiter))
                {
                    _waiter = null;
                }
            }
        }

        lock (_gate)
        {
            return TakeLocked(maxMessages, payloadLimit, frameBudget, clock(), ttl);
        }
    }

    private PopOutcome TakeLocked(int maxMessages, int payloadLimit, int frameBudget, DateTimeOffset now, TimeSpan? ttl)
    {
        DropExpiredLocked(now, ttl);
        var taken = new List<StoredMessage>();
        var used = 1 + ProtocolEncodingSize(Name) + 4;
        if (Kind == ChannelKind.Bag)
        {
            var skippedOversized = false;
            var node = _items.Last;
            while (node is not null && taken.Count < maxMessages)
            {
                var previous = node.Previous;
                if (!Fits(node.Value, payloadLimit, frameBudget, used))
                {
                    if (node.Value.Body.Length > payloadLimit)
                    {
                        skippedOversized = true;
                    }

                    node = previous;
                    continue;
                }

                used += MessageSize(node.Value);
                taken.Add(node.Value);
                _items.Remove(node);
                node = previous;
            }

            if (taken.Count == 0 && skippedOversized)
            {
                return new PopOutcome(tooLarge: true, taken);
            }

            return new PopOutcome(false, taken);
        }

        var fromFront = Kind == ChannelKind.Queue;
        while (taken.Count < maxMessages)
        {
            var node = fromFront ? _items.First : _items.Last;
            if (node is null)
            {
                break;
            }

            if (node.Value.Body.Length > payloadLimit)
            {
                if (taken.Count == 0)
                {
                    return new PopOutcome(tooLarge: true, []);
                }

                break;
            }

            var size = MessageSize(node.Value);
            if (used + size > frameBudget)
            {
                break;
            }

            used += size;
            taken.Add(node.Value);
            _items.Remove(node);
        }

        return new PopOutcome(false, taken);
    }

    private void DropExpiredLocked(DateTimeOffset now, TimeSpan? ttl)
    {
        if (ttl is null)
        {
            return;
        }

        while (_items.First is { } node && now - node.Value.EnqueuedAt >= ttl.Value)
        {
            _items.RemoveFirst();
        }
    }

    private static bool Fits(StoredMessage message, int payloadLimit, int frameBudget, int used) =>
        message.Body.Length <= payloadLimit && used + MessageSize(message) <= frameBudget;

    private static int MessageSize(StoredMessage message) =>
        ProtocolEncodingSize(message.ContentType) + 4 + message.Body.Length;

    private static int ProtocolEncodingSize(string value) => 2 + Encoding.UTF8.GetByteCount(value);
}
