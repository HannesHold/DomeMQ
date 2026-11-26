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
    private readonly IBrokerLogger? _logger;
    private readonly FullPolicy _defaultPolicy;
    private readonly ChannelKind _defaultKind;
    private readonly int _globalPayload;
    private readonly int _defaultQueue;
    private readonly long? _defaultMaxBytes;
    private readonly TimeSpan? _messageTtl;
    private readonly TimeSpan _orphanTtl;
    private readonly byte[][] _tokenHashes;
    private readonly bool _authRequired;
    private readonly QueueAccounting _accounting;
    private readonly Dictionary<string, ChannelLimitOverride> _overrides;
    private readonly object _gate = new();
    private readonly Dictionary<string, BrokerChannel> _channels = new(StringComparer.Ordinal);

    public BrokerEngine(DomeMqOptions options, IBrokerLogger? logger = null)
    {
        _options = options;
        _logger = logger;
        _defaultPolicy = DomeMqOptionsValidator.TryParseFullPolicy(options.Limits.FullBehavior, out var policy)
            ? policy
            : FullPolicy.Reject;
        DomeMqOptionsValidator.TryParseChannelKind(options.Channels.DefaultType, out _defaultKind);
        _globalPayload = options.Limits.MaxPayloadSize;
        _defaultQueue = options.Limits.MaxMessagesPerChannel;
        _defaultMaxBytes = options.Limits.MaxBytesPerChannel;
        _messageTtl = options.Channels.DefaultMessageTtl is TimeSpan ttl && ttl > TimeSpan.Zero ? ttl : null;
        _orphanTtl = options.Channels.OrphanChannelTtl;
        _authRequired = options.Authentication.Mode.Equals("SharedKey", StringComparison.OrdinalIgnoreCase);
        _tokenHashes = options.Authentication.Tokens.Select(token => SHA256.HashData(Encoding.UTF8.GetBytes(token))).ToArray();
        _overrides = options.Channels.Overrides
            .Where(static item => !string.IsNullOrWhiteSpace(item.Name))
            .GroupBy(static item => item.Name, StringComparer.Ordinal)
            .ToDictionary(static group => group.Key, static group => group.Last(), StringComparer.Ordinal);
        _accounting = new QueueAccounting(
            options.Limits.MaxQueuedBytes,
            options.Limits.MaxMessagesPerClient,
            options.Limits.WarningPercent)
        {
            OnPressure = PublishPressure
        };
    }

    internal event Action<QueuePressure>? Pressure;

    internal BrokerStatistics GetStatistics()
    {
        var accounting = _accounting.Snapshot();
        List<ChannelStatistics> channels;
        lock (_gate)
        {
            channels = _channels.Values.Select(static channel => channel.Statistics()).ToList();
        }

        return new BrokerStatistics
        {
            DroppedMessages = accounting.Dropped,
            ClearedMessages = accounting.Cleared,
            QueuedBytes = accounting.Bytes,
            QueuedMessages = channels.Sum(static channel => (long)channel.QueuedMessages),
            FillPercent = accounting.FillPercent,
            Channels = channels
        };
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
        var notices = new List<QueuePressure>();
        lock (_gate)
        {
            if (_orphanTtl <= TimeSpan.Zero)
            {
                foreach (var name in session.OwnedChannels)
                {
                    if (_channels.TryGetValue(name, out var channel))
                    {
                        notices.AddRange(channel.RemoveAndDrain());
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

        PublishAll(notices);
    }

    public void Cleanup() => Cleanup(Clock());

    public void Cleanup(DateTimeOffset now)
    {
        var notices = new List<QueuePressure>();
        lock (_gate)
        {
            List<string>? remove = null;
            foreach (var (name, channel) in _channels)
            {
                notices.AddRange(channel.DropExpired(now, _messageTtl));
                if (channel.OrphanDeadline is DateTimeOffset deadline && deadline <= now)
                {
                    remove ??= [];
                    remove.Add(name);
                }
            }

            if (remove is not null)
            {
                foreach (var name in remove)
                {
                    if (_channels.TryGetValue(name, out var channel))
                    {
                        notices.AddRange(channel.RemoveAndDrain());
                        _channels.Remove(name);
                    }
                }
            }
        }

        PublishAll(notices);
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

        _overrides.TryGetValue(body.Name, out var configured);
        int? configuredMessages = configured?.MaxMessages;
        int? requestedMessages = body.MaxQueueLength == 0 ? null : (int)body.MaxQueueLength;
        if (requestedMessages > _defaultQueue)
        {
            return Error(frame, ErrorCode.InvalidQueueLimit, "Requested MaxMessages exceeds the server maximum.");
        }

        var maxMessages = PayloadLimits.Min(_defaultQueue, configuredMessages, requestedMessages);
        long? configuredBytes = configured?.MaxBytes ?? _defaultMaxBytes;
        long? requestedBytes = body.MaxBytes == 0 ? null : body.MaxBytes;
        if (_defaultMaxBytes is long globalBytes && requestedBytes > globalBytes)
        {
            return Error(frame, ErrorCode.InvalidByteLimit, "Requested MaxBytes exceeds the server per-channel maximum.");
        }

        if (requestedBytes > _accounting.MaxQueuedBytes)
        {
            return Error(frame, ErrorCode.InvalidByteLimit, "Requested MaxBytes exceeds the server memory cap.");
        }

        long? maxBytes = MinLong(_defaultMaxBytes, configured?.MaxBytes, requestedBytes);
        if (!TryResolvePolicy(body.FullPolicy, configured, out var fullPolicy, out var policyError))
        {
            return Error(frame, ErrorCode.InvalidMessage, policyError);
        }

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

            var channel = new BrokerChannel(body.Name, kind, maxMessages, maxBytes, fullPolicy, channelPayload, session.Id, _accounting);
            _channels.Add(body.Name, channel);
            session.OwnedChannels.Add(body.Name);
        }

        var ack = MessageCodec.EncodeCreateChannelAck(new CreateChannelAck(
            body.Name,
            (byte)kind,
            (uint)maxMessages,
            (uint)(channelPayload ?? _globalPayload),
            (byte)fullPolicy,
            (uint)(maxBytes ?? 0)));
        return Reply(frame, MessageType.Ack, ack);
    }

    private EngineResult HandleDelete(Session session, Frame frame)
    {
        var body = MessageCodec.DecodeDeleteChannel(frame.Payload.Span);
        List<QueuePressure> drained = [];
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

            drained = channel.RemoveAndDrain();
            _channels.Remove(body.Name);
            session.OwnedChannels.Remove(body.Name);
        }

        PublishAll(drained);
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

        var pushed = channel.TryPush(new StoredMessage(body.Body, body.ContentType, Clock(), session.Id), Clock(), _messageTtl);
        PublishAll(pushed.Notices ?? []);
        if (pushed.Missing)
        {
            return Error(frame, ErrorCode.ChannelNotFound, "Channel was not found.");
        }

        if (pushed.Error is ErrorCode error)
        {
            return Error(frame, error, FullMessage(error));
        }

        if ((frame.Flags & FrameFlags.NoAck) != 0)
        {
            return default;
        }

        var status = pushed.Discarded ? PushStatus.Discarded : PushStatus.Accepted;
        var ack = MessageCodec.EncodePushAck(new PushAck(body.ChannelName, (uint)pushed.Depth, status));
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

    private bool TryResolvePolicy(byte requested, ChannelLimitOverride? configured, out FullPolicy policy, out string error)
    {
        if (requested != 0)
        {
            if (!Enum.IsDefined(typeof(FullPolicy), requested) || requested > (byte)FullPolicy.ClearAll)
            {
                policy = FullPolicy.Reject;
                error = "Unknown full policy.";
                return false;
            }

            policy = (FullPolicy)requested;
            error = "";
            return true;
        }

        if (!string.IsNullOrWhiteSpace(configured?.FullBehavior)
            && DomeMqOptionsValidator.TryParseFullPolicy(configured.FullBehavior, out var configuredPolicy))
        {
            policy = configuredPolicy;
            error = "";
            return true;
        }

        policy = _defaultPolicy;
        error = "";
        return true;
    }

    private static long? MinLong(params long?[] values)
    {
        long? result = null;
        foreach (var value in values)
        {
            if (value is not long candidate)
            {
                continue;
            }

            result = result is long current ? Math.Min(current, candidate) : candidate;
        }

        return result;
    }

    private static string FullMessage(ErrorCode error) => error switch
    {
        ErrorCode.ServerMemoryCap => "The server queued-byte cap has been reached.",
        ErrorCode.ClientQueueLimit => "This client has too many queued messages.",
        ErrorCode.ChannelFull => "Channel is full.",
        _ => "Channel is full."
    };

    private void PublishAll(IEnumerable<QueuePressure> notices)
    {
        foreach (var notice in notices)
        {
            PublishPressure(notice);
        }
    }

    private void PublishPressure(QueuePressure notice)
    {
        if (notice.Warning)
        {
            _logger?.Log(BrokerLogLevel.Warning, "Queue", $"{notice.Scope} is {notice.FillPercent}% full.");
        }

        Pressure?.Invoke(notice);
    }
}

internal sealed class StoredMessage(byte[] body, string contentType, DateTimeOffset enqueuedAt, Guid senderId)
{
    public byte[] Body { get; } = body;

    public string ContentType { get; } = contentType;

    public DateTimeOffset EnqueuedAt { get; } = enqueuedAt;

    public Guid SenderId { get; } = senderId;
}

internal readonly record struct ChannelPushResult
{
    public bool Missing { get; init; }

    public bool Discarded { get; init; }

    public ErrorCode? Error { get; init; }

    public int Depth { get; init; }

    public List<QueuePressure> Notices { get; init; }
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
    private readonly QueueAccounting _accounting;
    private TaskCompletionSource<bool>? _waiter;
    private long _bytes;
    private long _dropped;
    private long _cleared;
    private bool _warningActive;

    public BrokerChannel(
        string name,
        ChannelKind kind,
        int maxMessages,
        long? maxBytes,
        FullPolicy fullPolicy,
        int? maxPayloadSize,
        Guid ownerId,
        QueueAccounting accounting)
    {
        Name = name;
        Kind = kind;
        MaxMessages = maxMessages;
        MaxBytes = maxBytes;
        FullPolicy = fullPolicy;
        MaxPayloadSize = maxPayloadSize;
        OwnerId = ownerId;
        _accounting = accounting;
    }

    public string Name { get; }

    public ChannelKind Kind { get; }

    public int MaxMessages { get; }

    public long? MaxBytes { get; }

    public FullPolicy FullPolicy { get; }

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

    public List<QueuePressure> RemoveAndDrain()
    {
        var notices = new List<QueuePressure>();
        lock (_gate)
        {
            _removed = true;
            _waiter?.TrySetResult(false);
            _waiter = null;
            _accounting.Mutate(() =>
            {
                while (_items.First is { } node)
                {
                    DetachUnlocked(node, dropped: 0, cleared: 0);
                }

                notices.AddRange(TakeNoticesUnlocked());
            });
        }

        return notices;
    }

    public ChannelStatistics Statistics()
    {
        lock (_gate)
        {
            return new ChannelStatistics
            {
                Name = Name,
                Kind = Kind,
                FullPolicy = FullPolicy,
                QueuedMessages = _items.Count,
                QueuedBytes = _bytes,
                MaxMessages = MaxMessages,
                MaxBytes = MaxBytes,
                FillPercent = FillPercent(),
                DroppedMessages = _dropped,
                ClearedMessages = _cleared,
                WarningActive = _warningActive
            };
        }
    }

    public void Orphan(DateTimeOffset deadline)
    {
        lock (_gate)
        {
            _orphanDeadline = deadline;
        }
    }

    public List<QueuePressure> DropExpired(DateTimeOffset now, TimeSpan? ttl)
    {
        var notices = new List<QueuePressure>();
        lock (_gate)
        {
            _accounting.Mutate(() =>
            {
                DropExpiredUnlocked(now, ttl);
                notices.AddRange(TakeNoticesUnlocked());
            });
        }

        return notices;
    }

    public ChannelPushResult TryPush(StoredMessage message, DateTimeOffset now, TimeSpan? ttl)
    {
        TaskCompletionSource<bool>? wake = null;
        var notices = new List<QueuePressure>();
        ChannelPushResult result = default;
        lock (_gate)
        {
            if (Removed)
            {
                return new ChannelPushResult { Missing = true, Notices = notices };
            }

            _accounting.Mutate(() =>
            {
                DropExpiredUnlocked(now, ttl);
                result = ApplyPolicyUnlocked(message);
                notices.AddRange(TakeNoticesUnlocked());
                if (result.Error is null && !result.Discarded)
                {
                    wake = _waiter;
                    _waiter = null;
                }
            });
        }

        wake?.TrySetResult(true);
        return result with { Notices = notices };
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
        var firstNotices = new List<QueuePressure>();
        PopOutcome immediate = default;
        var finished = false;
        lock (_gate)
        {
            _accounting.Mutate(() =>
            {
                immediate = TakeLocked(maxMessages, payloadLimit, frameBudget, clock(), ttl);
                firstNotices.AddRange(TakeNoticesUnlocked());
            });
            if (immediate.TooLarge || immediate.Messages.Count > 0 || timeout <= TimeSpan.Zero || Removed)
            {
                finished = true;
            }
            else
            {
                waiter = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                _waiter = waiter;
            }
        }

        foreach (var notice in firstNotices)
        {
            _accounting.Publish(notice);
        }

        if (finished)
        {
            return immediate;
        }

        try
        {
            await waiter!.Task.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
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

        var secondNotices = new List<QueuePressure>();
        PopOutcome outcome = default;
        lock (_gate)
        {
            _accounting.Mutate(() =>
            {
                outcome = TakeLocked(maxMessages, payloadLimit, frameBudget, clock(), ttl);
                secondNotices.AddRange(TakeNoticesUnlocked());
            });
        }

        foreach (var notice in secondNotices)
        {
            _accounting.Publish(notice);
        }

        return outcome;
    }

    private PopOutcome TakeLocked(int maxMessages, int payloadLimit, int frameBudget, DateTimeOffset now, TimeSpan? ttl)
    {
        DropExpiredUnlocked(now, ttl);
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
            DetachUnlocked(node, dropped: 0, cleared: 0);
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
            DetachUnlocked(node, dropped: 0, cleared: 0);
        }

        return new PopOutcome(false, taken);
    }

    private void DropExpiredUnlocked(DateTimeOffset now, TimeSpan? ttl)
    {
        if (ttl is null)
        {
            return;
        }

        while (_items.First is { } node && now - node.Value.EnqueuedAt >= ttl.Value)
        {
            DetachUnlocked(node, dropped: 0, cleared: 0);
        }
    }

    private ChannelPushResult ApplyPolicyUnlocked(StoredMessage message)
    {
        var incoming = message.Body.Length;
        if (Fits(message.SenderId, incoming, removeBytes: 0, removeSenderMessages: 0, removeCount: 0))
        {
            AcceptUnlocked(message);
            return new ChannelPushResult { Depth = _items.Count };
        }

        switch (FullPolicy)
        {
            case FullPolicy.DiscardNewest:
                _dropped++;
                _accounting.NoteDroppedUnlocked(1);
                return new ChannelPushResult { Discarded = true, Depth = _items.Count };
            case FullPolicy.DiscardOldest:
            {
                var removed = new List<LinkedListNode<StoredMessage>>();
                var freedBytes = 0;
                var freedSender = 0;
                var node = _items.First;
                while (node is not null && !Fits(message.SenderId, incoming, freedBytes, freedSender, removed.Count))
                {
                    removed.Add(node);
                    freedBytes += node.Value.Body.Length;
                    if (node.Value.SenderId == message.SenderId)
                    {
                        freedSender++;
                    }

                    node = node.Next;
                }

                if (!Fits(message.SenderId, incoming, freedBytes, freedSender, removed.Count))
                {
                    return new ChannelPushResult { Error = Rejection(message.SenderId, incoming), Depth = _items.Count };
                }

                foreach (var item in removed)
                {
                    DetachUnlocked(item, dropped: 1, cleared: 0);
                }

                AcceptUnlocked(message);
                return new ChannelPushResult { Depth = _items.Count };
            }
            case FullPolicy.ClearAll:
            {
                var freedBytes = (int)_bytes;
                var freedSender = _items.Count(item => item.SenderId == message.SenderId);
                if (!Fits(message.SenderId, incoming, freedBytes, freedSender, _items.Count))
                {
                    return new ChannelPushResult { Error = Rejection(message.SenderId, incoming), Depth = _items.Count };
                }

                while (_items.First is { } first)
                {
                    DetachUnlocked(first, dropped: 0, cleared: 1);
                }

                AcceptUnlocked(message);
                return new ChannelPushResult { Depth = _items.Count };
            }
            default:
                return new ChannelPushResult { Error = Rejection(message.SenderId, incoming), Depth = _items.Count };
        }
    }

    private bool Fits(Guid sender, int incoming, int removeBytes, int removeSenderMessages, int removeCount)
    {
        if (_items.Count - removeCount + 1 > MaxMessages)
        {
            return false;
        }

        if (MaxBytes is long cap && _bytes - removeBytes + incoming > cap)
        {
            return false;
        }

        return _accounting.FitsUnlocked(sender, 1 - removeSenderMessages, incoming - removeBytes);
    }

    private ErrorCode Rejection(Guid sender, int incoming)
    {
        if (MaxBytes is long cap && incoming > cap)
        {
            return ErrorCode.ChannelFull;
        }

        if (_items.Count + 1 > MaxMessages && FullPolicy == FullPolicy.Reject)
        {
            return ErrorCode.ChannelFull;
        }

        var account = _accounting.RejectionUnlocked(sender, 1, incoming);
        if (account != ErrorCode.ChannelFull)
        {
            return account;
        }

        if (MaxBytes is long && _bytes + incoming > MaxBytes)
        {
            return ErrorCode.ChannelFull;
        }

        return ErrorCode.ChannelFull;
    }

    private void AcceptUnlocked(StoredMessage message)
    {
        _items.AddLast(message);
        _bytes += message.Body.Length;
        _accounting.AddUnlocked(message.SenderId, message.Body.Length);
    }

    private void DetachUnlocked(LinkedListNode<StoredMessage> node, int dropped, int cleared)
    {
        var message = node.Value;
        _items.Remove(node);
        _bytes -= message.Body.Length;
        _dropped += dropped;
        _cleared += cleared;
        _accounting.ReleaseUnlocked(message.SenderId, message.Body.Length, dropped, cleared);
    }

    private int FillPercent()
    {
        var messages = MaxMessages <= 0 ? 0 : (int)Math.Min(100, _items.Count * 100L / MaxMessages);
        var bytes = MaxBytes is long cap && cap > 0 ? (int)Math.Min(100, _bytes * 100 / cap) : 0;
        return Math.Max(messages, bytes);
    }

    private List<QueuePressure> TakeNoticesUnlocked()
    {
        var notices = new List<QueuePressure>();
        var fill = FillPercent();
        if (fill >= _accounting.WarningPercent)
        {
            if (!_warningActive)
            {
                _warningActive = true;
                var snapshot = _accounting.SnapshotUnlocked();
                notices.Add(new QueuePressure
                {
                    Scope = Name,
                    FillPercent = fill,
                    Warning = true,
                    DroppedMessages = _dropped,
                    ClearedMessages = _cleared,
                    QueuedMessages = _items.Count,
                    QueuedBytes = _bytes
                });
                _ = snapshot;
            }
        }
        else
        {
            _warningActive = false;
        }

        notices.AddRange(_accounting.DrainNoticesUnlocked());
        return notices;
    }

    private static bool Fits(StoredMessage message, int payloadLimit, int frameBudget, int used) =>
        message.Body.Length <= payloadLimit && used + MessageSize(message) <= frameBudget;

    private static int MessageSize(StoredMessage message) =>
        ProtocolEncodingSize(message.ContentType) + 4 + message.Body.Length;

    private static int ProtocolEncodingSize(string value) => 2 + Encoding.UTF8.GetByteCount(value);
}
