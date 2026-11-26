using DomeMQ;

namespace DomeMQ.Server;

public sealed class QueuePressure
{
    public required string Scope { get; init; }

    public required int FillPercent { get; init; }

    public required bool Warning { get; init; }

    public required long DroppedMessages { get; init; }

    public required long ClearedMessages { get; init; }

    public required long QueuedMessages { get; init; }

    public required long QueuedBytes { get; init; }
}

public sealed class ChannelStatistics
{
    public required string Name { get; init; }

    public required ChannelKind Kind { get; init; }

    public required FullPolicy FullPolicy { get; init; }

    public int QueuedMessages { get; init; }

    public long QueuedBytes { get; init; }

    public int MaxMessages { get; init; }

    public long? MaxBytes { get; init; }

    public int FillPercent { get; init; }

    public long DroppedMessages { get; init; }

    public long ClearedMessages { get; init; }

    public bool WarningActive { get; init; }
}

public sealed class BrokerStatistics
{
    public long DroppedMessages { get; init; }

    public long ClearedMessages { get; init; }

    public long QueuedMessages { get; init; }

    public long QueuedBytes { get; init; }

    public int FillPercent { get; init; }

    public IReadOnlyList<ChannelStatistics> Channels { get; init; } = [];
}

internal sealed class QueueAccounting
{
    private readonly object _gate = new();
    private readonly Dictionary<Guid, ClientCounter> _clients = [];
    private readonly List<QueuePressure> _pending = [];
    private long _bytes;
    private long _dropped;
    private long _cleared;
    private bool _memoryWarning;

    public QueueAccounting(long maxQueuedBytes, int maxMessagesPerClient, int warningPercent)
    {
        MaxQueuedBytes = maxQueuedBytes;
        MaxMessagesPerClient = maxMessagesPerClient;
        WarningPercent = warningPercent;
    }

    public long MaxQueuedBytes { get; }

    public int MaxMessagesPerClient { get; }

    public int WarningPercent { get; }

    public Action<QueuePressure>? OnPressure { get; set; }

    public void Mutate(Action action)
    {
        lock (_gate)
        {
            action();
        }
    }

    public bool FitsUnlocked(Guid sender, int additionalMessages, int additionalBytes)
    {
        if (additionalBytes > MaxQueuedBytes)
        {
            return false;
        }

        if (_bytes + additionalBytes > MaxQueuedBytes)
        {
            return false;
        }

        var count = _clients.TryGetValue(sender, out var existing) ? existing.Count : 0;
        return count + additionalMessages <= MaxMessagesPerClient;
    }

    public ErrorCode RejectionUnlocked(Guid sender, int additionalMessages, int additionalBytes)
    {
        if (additionalBytes > MaxQueuedBytes || _bytes + additionalBytes > MaxQueuedBytes)
        {
            return ErrorCode.ServerMemoryCap;
        }

        var count = _clients.TryGetValue(sender, out var existing) ? existing.Count : 0;
        if (count + additionalMessages > MaxMessagesPerClient)
        {
            return ErrorCode.ClientQueueLimit;
        }

        return ErrorCode.ChannelFull;
    }

    public void AddUnlocked(Guid sender, int bytes) => ChangeUnlocked(sender, bytes, 1, 0, 0);

    public void ReleaseUnlocked(Guid sender, int bytes, int dropped, int cleared) =>
        ChangeUnlocked(sender, -bytes, -1, dropped, cleared);

    public void NoteDroppedUnlocked(int dropped) => _dropped += dropped;

    public List<QueuePressure> DrainNoticesUnlocked()
    {
        var notices = new List<QueuePressure>(_pending);
        _pending.Clear();
        var memory = MemoryNoticeUnlocked();
        if (memory is not null)
        {
            notices.Add(memory);
        }

        return notices;
    }

    public (long Bytes, long Dropped, long Cleared, int FillPercent) SnapshotUnlocked()
    {
        var fill = Percent(_bytes, MaxQueuedBytes);
        return (_bytes, _dropped, _cleared, fill);
    }

    public (long Bytes, long Dropped, long Cleared, int FillPercent) Snapshot()
    {
        lock (_gate)
        {
            return SnapshotUnlocked();
        }
    }

    public QueuePressure? MemoryNoticeUnlocked()
    {
        var fill = Percent(_bytes, MaxQueuedBytes);
        if (fill >= WarningPercent)
        {
            if (_memoryWarning)
            {
                return null;
            }

            _memoryWarning = true;
            return Notice("server", fill, _dropped, _cleared, 0, _bytes);
        }

        _memoryWarning = false;
        return null;
    }

    public void Publish(QueuePressure? notice)
    {
        if (notice is not null)
        {
            OnPressure?.Invoke(notice);
        }
    }

    private void ChangeUnlocked(Guid sender, int bytes, int messages, int dropped, int cleared)
    {
        _bytes += bytes;
        if (_bytes < 0)
        {
            _bytes = 0;
        }

        _dropped += dropped;
        _cleared += cleared;
        if (!_clients.TryGetValue(sender, out var counter))
        {
            if (messages <= 0)
            {
                return;
            }

            counter = new ClientCounter();
            _clients.Add(sender, counter);
        }

        counter.Count += messages;
        if (counter.Count <= 0)
        {
            _clients.Remove(sender);
            return;
        }

        var fill = Percent(counter.Count, MaxMessagesPerClient);
        if (fill >= WarningPercent)
        {
            if (!counter.Warning)
            {
                counter.Warning = true;
                _pending.Add(Notice("client:" + sender.ToString("N"), fill, _dropped, _cleared, counter.Count, _bytes));
            }
        }
        else
        {
            counter.Warning = false;
        }
    }

    private static int Percent(long used, long cap)
    {
        if (cap <= 0 || used <= 0)
        {
            return 0;
        }

        return (int)Math.Min(100, used * 100 / cap);
    }

    private static QueuePressure Notice(string scope, int fill, long dropped, long cleared, long messages, long bytes) =>
        new()
        {
            Scope = scope,
            FillPercent = fill,
            Warning = true,
            DroppedMessages = dropped,
            ClearedMessages = cleared,
            QueuedMessages = messages,
            QueuedBytes = bytes
        };

    private sealed class ClientCounter
    {
        public int Count { get; set; }

        public bool Warning { get; set; }
    }
}
