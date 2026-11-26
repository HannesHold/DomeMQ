using System.Collections.Concurrent;
using DomeMQ;

namespace DomeMQ.Inspector.Services;

public sealed class TrafficFrame
{
    public long Id { get; init; }

    public DateTimeOffset Timestamp { get; init; }

    public TrafficDirection Direction { get; init; }

    public string Endpoint { get; init; } = "";

    public int Length { get; init; }

    public byte[] Bytes { get; init; } = [];

    public bool IsFrame { get; init; }

    public byte Version { get; init; }

    public byte Type { get; init; }

    public string TypeName { get; init; } = "Raw";

    public ushort Flags { get; init; }

    public uint RequestId { get; init; }

    public uint PayloadLength { get; init; }

    public string Channel { get; init; } = "";

    public string Summary { get; init; } = "";

    public string ErrorCode { get; init; } = "";

    public string DirectionLabel => Direction == TrafficDirection.Inbound ? "In" : "Out";

    public string TimeLabel => Timestamp.ToString("HH:mm:ss.fff");
}

public sealed class TrafficLog : ITrafficObserver
{
    private readonly ConcurrentQueue<TrafficChunk> _incoming = new();
    private readonly Dictionary<string, List<byte>> _streams = new(StringComparer.Ordinal);
    private readonly List<TrafficFrame> _frames = [];
    private readonly Dictionary<string, EndpointTotals> _endpoints = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _errors = new(StringComparer.Ordinal);
    private long _nextId = 1;
    private int _queued;

    public int Capacity { get; set; } = 2000;

    public bool Paused { get; set; }

    public bool Reassemble
    {
        get => _reassemble;
        set
        {
            _reassemble = value;
            _streams.Clear();
        }
    }

    private bool _reassemble = true;

    public bool AutoScroll { get; set; } = true;

    public string DirectionFilter { get; set; } = "Any";

    public string TypeFilter { get; set; } = "";

    public string EndpointFilter { get; set; } = "";

    public string ChannelFilter { get; set; } = "";

    public string RequestFilter { get; set; } = "";

    public long? FocusFrom { get; set; }

    public long? FocusTo { get; set; }

    public long TotalBytes { get; private set; }

    public long TotalFrames { get; private set; }

    public int DroppedChunks { get; private set; }

    public long Mark() => _nextId;

    public void OnChunk(TrafficChunk chunk)
    {
        _incoming.Enqueue(chunk);
        if (Interlocked.Increment(ref _queued) > 8000 && _incoming.TryDequeue(out _))
        {
            Interlocked.Decrement(ref _queued);
            DroppedChunks++;
        }
    }

    public DrainBatch Drain(int max)
    {
        var added = new List<TrafficFrame>();
        var taken = 0;
        while (taken < max && _incoming.TryDequeue(out var chunk))
        {
            Interlocked.Decrement(ref _queued);
            taken++;
            TotalBytes += chunk.Data.Length;
            Note(chunk.Endpoint, chunk.Direction, chunk.Data.Length, message: false);
            if (Reassemble)
            {
                added.AddRange(Consume(chunk));
            }
            else
            {
                added.Add(Raw(chunk, chunk.Data, "chunk"));
            }
        }

        long trimTo = 0;
        var cap = Math.Max(200, Capacity);
        if (_frames.Count > cap)
        {
            var remove = _frames.Count - cap;
            trimTo = _frames[remove - 1].Id + 1;
            _frames.RemoveRange(0, remove);
        }

        if (Paused)
        {
            return new DrainBatch([], trimTo);
        }

        return new DrainBatch(added.Where(Matches).ToArray(), trimTo);
    }

    public IReadOnlyList<TrafficFrame> Filtered() => _frames.Where(Matches).ToArray();

    public string ExportText()
    {
        var writer = new StringWriter();
        foreach (var frame in Filtered())
        {
            writer.WriteLine($"{frame.TimeLabel} {frame.DirectionLabel} {frame.Endpoint} {frame.Length} bytes {frame.TypeName} req={frame.RequestId} {frame.Summary}");
            foreach (var row in HexFormat.Rows(frame.Bytes, frame.IsFrame ? ProtocolConstants.HeaderSize : 0))
            {
                writer.Write(row.Offset);
                writer.Write("  ");
                writer.WriteLine(string.Join(" ", row.Cells.Select(static cell => cell.Text)).PadRight(16 * 3) + " " + row.Ascii);
            }

            writer.WriteLine();
        }

        return writer.ToString();
    }

    public (long InBytes, long OutBytes, long InMessages, long OutMessages) Counters(string endpoint)
    {
        if (!_endpoints.TryGetValue(endpoint, out var totals))
        {
            return default;
        }

        return (totals.InBytes, totals.OutBytes, totals.InMessages, totals.OutMessages);
    }

    public IReadOnlyDictionary<string, int> ErrorCounts() => new Dictionary<string, int>(_errors, StringComparer.Ordinal);

    private void Note(string endpoint, TrafficDirection direction, int bytes, bool message)
    {
        if (!_endpoints.TryGetValue(endpoint, out var totals))
        {
            totals = new EndpointTotals();
            _endpoints[endpoint] = totals;
        }

        if (direction == TrafficDirection.Inbound)
        {
            totals.InBytes += bytes;
            if (message)
            {
                totals.InMessages++;
            }
        }
        else
        {
            totals.OutBytes += bytes;
            if (message)
            {
                totals.OutMessages++;
            }
        }
    }

    private List<TrafficFrame> Consume(TrafficChunk chunk)
    {
        var key = chunk.Endpoint + "|" + (int)chunk.Direction;
        if (!_streams.TryGetValue(key, out var buffer))
        {
            buffer = [];
            _streams[key] = buffer;
        }

        buffer.AddRange(chunk.Data);
        var produced = new List<TrafficFrame>();
        while (buffer.Count >= ProtocolConstants.HeaderSize)
        {
            var header = buffer.GetRange(0, ProtocolConstants.HeaderSize).ToArray();
            if (!FrameCodec.TryReadHeader(header, out var decoded, out _))
            {
                var magic = IndexOfMagic(buffer);
                if (magic < 0)
                {
                    if (buffer.Count > 65536)
                    {
                        produced.Add(Raw(chunk, buffer.ToArray(), "no magic"));
                        buffer.Clear();
                    }

                    break;
                }

                if (magic > 0)
                {
                    produced.Add(Raw(chunk, buffer.Take(magic).ToArray(), "skipped"));
                    buffer.RemoveRange(0, magic);
                }

                continue;
            }

            if (decoded.PayloadLength > ProtocolConstants.MaxConfigurablePayload)
            {
                buffer.RemoveAt(0);
                continue;
            }

            var total = ProtocolConstants.HeaderSize + (int)decoded.PayloadLength;
            if (buffer.Count < total)
            {
                break;
            }

            var bytes = buffer.GetRange(0, total).ToArray();
            buffer.RemoveRange(0, total);
            produced.Add(Frame(chunk, bytes, decoded));
        }

        return produced;
    }

    private TrafficFrame Frame(TrafficChunk chunk, byte[] bytes, FrameHeader header)
    {
        TotalFrames++;
        Note(chunk.Endpoint, chunk.Direction, 0, message: true);
        var payload = bytes.AsSpan(ProtocolConstants.HeaderSize);
        var typeName = Enum.IsDefined(typeof(MessageType), header.Type) ? ((MessageType)header.Type).ToString() : $"0x{header.Type:X2}";
        var frame = new TrafficFrame
        {
            Id = _nextId++,
            Timestamp = chunk.Timestamp,
            Direction = chunk.Direction,
            Endpoint = chunk.Endpoint,
            Length = bytes.Length,
            Bytes = bytes,
            IsFrame = true,
            Version = header.Version,
            Type = header.Type,
            TypeName = typeName,
            Flags = header.Flags,
            RequestId = header.RequestId,
            PayloadLength = header.PayloadLength,
            Summary = typeName
        };
        frame = Describe(frame, payload);
        _frames.Add(frame);
        return frame;
    }

    private TrafficFrame Raw(TrafficChunk chunk, byte[] bytes, string reason)
    {
        var frame = new TrafficFrame
        {
            Id = _nextId++,
            Timestamp = chunk.Timestamp,
            Direction = chunk.Direction,
            Endpoint = chunk.Endpoint,
            Length = bytes.Length,
            Bytes = bytes,
            IsFrame = false,
            TypeName = "Raw",
            Summary = reason
        };
        _frames.Add(frame);
        return frame;
    }

    private TrafficFrame Describe(TrafficFrame frame, ReadOnlySpan<byte> payload)
    {
        try
        {
            switch ((MessageType)frame.Type)
            {
                case MessageType.Push:
                    var push = MessageCodec.DecodePush(payload);
                    return With(frame, push.ChannelName, $"push {push.ChannelName} ({push.Body.Length} bytes)");
                case MessageType.Pop:
                    var pop = MessageCodec.DecodePop(payload);
                    return With(frame, pop.ChannelName, $"pop {pop.ChannelName}");
                case MessageType.CreateChannel:
                    var create = MessageCodec.DecodeCreateChannel(payload);
                    return With(frame, create.Name, $"create {create.Name}");
                case MessageType.DeleteChannel:
                    var delete = MessageCodec.DecodeDeleteChannel(payload);
                    return With(frame, delete.Name, $"delete {delete.Name}");
                case MessageType.Error:
                    var error = MessageCodec.DecodeError(payload);
                    var code = Enum.IsDefined(typeof(ErrorCode), error.Code) ? ((ErrorCode)error.Code).ToString() : error.Code.ToString();
                    _errors[code] = _errors.GetValueOrDefault(code) + 1;
                    return With(frame, "", $"{code}: {error.Message}", code);
                case MessageType.Ack when payload.Length > 0 && payload[0] == (byte)MessageType.Push:
                    var pushAck = MessageCodec.DecodePushAck(payload);
                    var status = pushAck.Status == PushStatus.Discarded ? "discarded" : "accepted";
                    return With(frame, pushAck.Name, $"ack push {pushAck.Name} {status} depth {pushAck.Depth}");
                case MessageType.Ack when payload.Length > 0 && payload[0] == (byte)MessageType.CreateChannel:
                    var created = MessageCodec.DecodeCreateChannelAck(payload);
                    return With(frame, created.Name, $"ack create {created.Name}");
                case MessageType.Ack when payload.Length > 0 && payload[0] == (byte)MessageType.Pop:
                    var popped = MessageCodec.DecodePopAck(payload);
                    return With(frame, popped.Name, $"ack pop {popped.Name} x{popped.Messages.Length}");
                case MessageType.Connect:
                    var connect = MessageCodec.DecodeConnect(payload);
                    return With(frame, "", $"connect {connect.ClientName}");
                case MessageType.Hello:
                    var hello = MessageCodec.DecodeHello(payload);
                    return With(frame, "", $"hello {hello.ServerName}");
            }
        }
        catch (Exception ex)
        {
            return With(frame, frame.Channel, frame.TypeName + " (" + ex.Message + ")");
        }

        return frame;
    }

    private static TrafficFrame With(TrafficFrame frame, string channel, string summary, string error = "") => new()
    {
        Id = frame.Id,
        Timestamp = frame.Timestamp,
        Direction = frame.Direction,
        Endpoint = frame.Endpoint,
        Length = frame.Length,
        Bytes = frame.Bytes,
        IsFrame = frame.IsFrame,
        Version = frame.Version,
        Type = frame.Type,
        TypeName = frame.TypeName,
        Flags = frame.Flags,
        RequestId = frame.RequestId,
        PayloadLength = frame.PayloadLength,
        Channel = channel,
        Summary = summary,
        ErrorCode = error
    };

    private bool Matches(TrafficFrame frame)
    {
        if (FocusFrom is long from && frame.Id < from)
        {
            return false;
        }

        if (FocusTo is long to && frame.Id >= to)
        {
            return false;
        }

        if (DirectionFilter == "In" && frame.Direction != TrafficDirection.Inbound)
        {
            return false;
        }

        if (DirectionFilter == "Out" && frame.Direction != TrafficDirection.Outbound)
        {
            return false;
        }

        if (!Contains(frame.Endpoint, EndpointFilter) || !Contains(frame.Channel, ChannelFilter))
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(TypeFilter)
            && !frame.TypeName.Contains(TypeFilter.Trim(), StringComparison.OrdinalIgnoreCase)
            && !frame.Type.ToString().Contains(TypeFilter.Trim(), StringComparison.Ordinal))
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(RequestFilter)
            && !frame.RequestId.ToString().Contains(RequestFilter.Trim(), StringComparison.Ordinal))
        {
            return false;
        }

        return true;
    }

    private static bool Contains(string value, string filter) =>
        string.IsNullOrWhiteSpace(filter) || value.Contains(filter.Trim(), StringComparison.OrdinalIgnoreCase);

    private static int IndexOfMagic(List<byte> data)
    {
        var magic = ProtocolConstants.Magic;
        for (var i = 0; i <= data.Count - magic.Length; i++)
        {
            var found = true;
            for (var j = 0; j < magic.Length; j++)
            {
                if (data[i + j] != magic[j])
                {
                    found = false;
                    break;
                }
            }

            if (found)
            {
                return i;
            }
        }

        return -1;
    }

    private sealed class EndpointTotals
    {
        public long InBytes;
        public long OutBytes;
        public long InMessages;
        public long OutMessages;
    }
}

public readonly record struct DrainBatch(IReadOnlyList<TrafficFrame> Added, long TrimBeforeId);
