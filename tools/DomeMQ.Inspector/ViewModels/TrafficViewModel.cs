using System.Collections.ObjectModel;
using Avalonia.Media;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using DomeMQ;
using DomeMQ.Inspector.Services;
using DomeMQ.Server;

namespace DomeMQ.Inspector.ViewModels;

public partial class TrafficViewModel : ObservableObject
{
    private readonly TrafficLog _log;

    public TrafficViewModel(TrafficLog log)
    {
        _log = log;
        Entries = [];
    }

    public ObservableCollection<TrafficFrame> Entries { get; }

    public string[] Directions { get; } = ["Any", "In", "Out"];

    [ObservableProperty]
    private TrafficFrame? _selected;

    [ObservableProperty]
    private string _direction = "Any";

    [ObservableProperty]
    private string _typeFilter = "";

    [ObservableProperty]
    private string _endpointFilter = "";

    [ObservableProperty]
    private string _channelFilter = "";

    [ObservableProperty]
    private string _requestFilter = "";

    [ObservableProperty]
    private bool _paused;

    [ObservableProperty]
    private bool _autoScroll = true;

    [ObservableProperty]
    private bool _reassemble = true;

    [ObservableProperty]
    private int _capacity = 2000;

    [ObservableProperty]
    private string _headerText = "Select a row to decode the frame.";

    [ObservableProperty]
    private string _truncated = "";

    [ObservableProperty]
    private string _exportPath = "";

    [ObservableProperty]
    private string _status = "0 frames";

    public ObservableCollection<HexRow> Rows { get; } = [];

    public event Action? ScrollRequested;

    public void Apply(DrainBatch batch)
    {
        if (batch.TrimBeforeId > 0)
        {
            while (Entries.Count > 0 && Entries[0].Id < batch.TrimBeforeId)
            {
                Entries.RemoveAt(0);
            }
        }

        foreach (var frame in batch.Added)
        {
            Entries.Add(frame);
        }

        if (batch.Added.Count > 0 && AutoScroll && !Paused)
        {
            ScrollRequested?.Invoke();
        }

        Status = $"{Entries.Count} shown, buffer {_log.Capacity}, dropped chunks {_log.DroppedChunks}";
    }

    public void Reload()
    {
        Entries.Clear();
        foreach (var frame in _log.Filtered())
        {
            Entries.Add(frame);
        }

        if (AutoScroll)
        {
            ScrollRequested?.Invoke();
        }
    }

    public void ShowRange(long from, long to)
    {
        _log.FocusFrom = from;
        _log.FocusTo = to;
        RequestFilter = "";
        Reload();
    }

    partial void OnSelectedChanged(TrafficFrame? value)
    {
        Rows.Clear();
        if (value is null)
        {
            HeaderText = "Select a row to decode the frame.";
            Truncated = "";
            return;
        }

        var headerLength = value.IsFrame ? ProtocolConstants.HeaderSize : 0;
        foreach (var row in HexFormat.Rows(value.Bytes, headerLength))
        {
            Rows.Add(row);
        }

        Truncated = value.Bytes.Length > HexFormat.DetailCap
            ? $"Showing the first {HexFormat.DetailCap} of {value.Bytes.Length} bytes."
            : "";
        if (!value.IsFrame)
        {
            HeaderText = $"{value.TimeLabel}  {value.DirectionLabel}  {value.Endpoint}  {value.Length} bytes  raw ({value.Summary})";
            return;
        }

        var flags = value.Flags == FrameFlags.None ? "none" : $"0x{value.Flags:X4}" + ((value.Flags & FrameFlags.NoAck) != 0 ? " NoAck" : "");
        HeaderText =
            $"magic 44 4D 51 01    version {value.Version}    type {value.Type} {value.TypeName}    flags {flags}\n" +
            $"request id {value.RequestId}    payload length {value.PayloadLength}    channel {(string.IsNullOrEmpty(value.Channel) ? "—" : value.Channel)}\n" +
            $"{value.Summary}";
    }

    partial void OnPausedChanged(bool value)
    {
        _log.Paused = value;
        if (!value)
        {
            Reload();
        }
    }

    partial void OnReassembleChanged(bool value)
    {
        _log.Reassemble = value;
        Reload();
    }

    partial void OnCapacityChanged(int value)
    {
        _log.Capacity = Math.Max(200, value);
    }

    [RelayCommand]
    private void ApplyFilters()
    {
        _log.DirectionFilter = Direction;
        _log.TypeFilter = TypeFilter;
        _log.EndpointFilter = EndpointFilter;
        _log.ChannelFilter = ChannelFilter;
        _log.RequestFilter = RequestFilter;
        _log.FocusFrom = null;
        _log.FocusTo = null;
        Reload();
    }

    [RelayCommand]
    private void Clear()
    {
        Entries.Clear();
        Selected = null;
    }

    [RelayCommand]
    private void Export()
    {
        var path = string.IsNullOrWhiteSpace(ExportPath)
            ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "domemq-traffic.txt")
            : ExportPath;
        File.WriteAllText(path, _log.ExportText());
        ExportPath = path;
        Status = "Exported " + path;
    }

    public void ExportTo(string path)
    {
        File.WriteAllText(path, _log.ExportText());
        ExportPath = path;
        Status = "Exported " + path;
    }
}

public partial class ClientRow : ObservableObject
{
    public string Id { get; set; } = "";

    [ObservableProperty]
    private string _endpoint = "";

    [ObservableProperty]
    private string _connectedAt = "";

    [ObservableProperty]
    private string _certificate = "";

    [ObservableProperty]
    private string _keyId = "";

    [ObservableProperty]
    private string _state = "";

    [ObservableProperty]
    private string _messages = "";

    [ObservableProperty]
    private string _bytes = "";
}

public partial class ChannelRow : ObservableObject
{
    [ObservableProperty]
    private string _name = "";

    [ObservableProperty]
    private string _kind = "";

    [ObservableProperty]
    private string _owner = "";

    [ObservableProperty]
    private string _count = "";

    [ObservableProperty]
    private string _bytes = "";

    [ObservableProperty]
    private int _fillPercent;

    [ObservableProperty]
    private IBrush _fillBrush = SolidColorBrush.Parse("#6CB6FF");

    [ObservableProperty]
    private string _policy = "";

    [ObservableProperty]
    private string _dropped = "";
}

public partial class ServerViewModel : ObservableObject
{
    private readonly ServerHost _host;
    private readonly TrafficLog _log;
    private readonly Queue<double> _rates = new();
    private long _lastFrames;
    private long _lastBytes;
    private DateTimeOffset _lastSample = DateTimeOffset.UtcNow;
    private int _pressureSeen;

    public ServerViewModel(ServerHost host, TrafficLog log)
    {
        _host = host;
        _log = log;
        Clients = [];
        Channels = [];
        Pressure = [];
        ConfigPath = LaunchOptions.ConfigPath ?? "";
        Rates = [];
    }

    public ObservableCollection<ClientRow> Clients { get; }

    public ObservableCollection<ChannelRow> Channels { get; }

    public ObservableCollection<string> Pressure { get; }

    public string[] KindChoices { get; } = [];

    [ObservableProperty]
    private string _configPath;

    [ObservableProperty]
    private string _status = "Stopped";

    [ObservableProperty]
    private string _configSummary = "";

    [ObservableProperty]
    private string _messagesPerSecond = "0";

    [ObservableProperty]
    private string _bytesPerSecond = "0";

    [ObservableProperty]
    private string _queuedText = "0 / 0";

    [ObservableProperty]
    private int _queuedPercent;

    [ObservableProperty]
    private IBrush _queuedBrush = SolidColorBrush.Parse("#6CB6FF");

    [ObservableProperty]
    private string _errorSummary = "No errors";

    [ObservableProperty]
    private string _droppedText = "0 dropped, 0 cleared";

    [ObservableProperty]
    private bool _running;

    public double[] Rates { get; private set; }

    public event Action? ChartUpdated;

    [RelayCommand]
    private async Task Start()
    {
        try
        {
            Status = "Starting…";
            await _host.StartAsync(string.IsNullOrWhiteSpace(ConfigPath) ? null : ConfigPath, _log, CancellationToken.None);
            Status = _host.Status;
            ConfigSummary = _host.ConfigSummary;
            Running = true;
        }
        catch (Exception ex)
        {
            Status = ex.Message;
            Running = false;
        }
    }

    [RelayCommand]
    private async Task Stop()
    {
        await _host.StopAsync();
        Running = false;
        Status = _host.Status;
        Clients.Clear();
        Channels.Clear();
        Pressure.Clear();
        _pressureSeen = 0;
    }

    public void Refresh()
    {
        if (!Running && !_host.Running)
        {
            return;
        }

        Running = _host.Running;
        var stats = _host.Statistics();
        if (stats is not null)
        {
            var max = Math.Max(1, _host.MaxQueuedBytes);
            QueuedPercent = (int)Math.Clamp(stats.QueuedBytes * 100 / max, 0, 100);
            QueuedText = $"{Format.Bytes(stats.QueuedBytes)} queued of {Format.Bytes(max)}";
            QueuedBrush = QueuedPercent >= 80 ? SolidColorBrush.Parse("#F0B429") : SolidColorBrush.Parse("#6CB6FF");
            DroppedText = $"{stats.DroppedMessages} dropped, {stats.ClearedMessages} cleared";
            SyncChannels(stats.Channels);
        }

        SyncClients();
        SyncPressure();
        SampleRates();
        var errors = _log.ErrorCounts();
        ErrorSummary = errors.Count == 0
            ? "No errors"
            : string.Join("   ", errors.OrderBy(static pair => pair.Key).Select(static pair => $"{pair.Key} {pair.Value}"));
    }

    private void SampleRates()
    {
        var now = DateTimeOffset.UtcNow;
        var seconds = (now - _lastSample).TotalSeconds;
        if (seconds < 0.4)
        {
            return;
        }

        var frames = _log.TotalFrames;
        var bytes = _log.TotalBytes;
        var messageRate = (frames - _lastFrames) / seconds;
        var byteRate = (bytes - _lastBytes) / seconds;
        _lastFrames = frames;
        _lastBytes = bytes;
        _lastSample = now;
        MessagesPerSecond = messageRate.ToString("0.0");
        BytesPerSecond = Format.Bytes(byteRate) + "/s";
        _rates.Enqueue(messageRate);
        while (_rates.Count > 60)
        {
            _rates.Dequeue();
        }

        Rates = _rates.ToArray();
        ChartUpdated?.Invoke();
    }

    private void SyncClients()
    {
        var snapshots = _host.Clients();
        var known = Clients.ToDictionary(static row => row.Id, StringComparer.Ordinal);
        var live = new HashSet<string>(StringComparer.Ordinal);
        foreach (var snapshot in snapshots)
        {
            live.Add(snapshot.Id);
            if (!known.TryGetValue(snapshot.Id, out var row))
            {
                row = new ClientRow { Id = snapshot.Id };
                Clients.Add(row);
                known[snapshot.Id] = row;
            }

            var counters = _log.Counters(snapshot.Endpoint);
            row.Endpoint = snapshot.Endpoint;
            row.ConnectedAt = snapshot.ConnectedAt.ToLocalTime().ToString("HH:mm:ss");
            row.Certificate = string.IsNullOrEmpty(snapshot.Certificate) ? "—" : snapshot.Certificate;
            row.KeyId = string.IsNullOrEmpty(snapshot.KeyId) ? "—" : snapshot.KeyId;
            row.State = snapshot.Connected ? "connected" : "closed";
            row.Messages = $"{counters.InMessages} in / {counters.OutMessages} out";
            row.Bytes = $"{Format.Bytes(counters.InBytes)} in / {Format.Bytes(counters.OutBytes)} out";
        }

        for (var i = Clients.Count - 1; i >= 0; i--)
        {
            if (!live.Contains(Clients[i].Id))
            {
                Clients.RemoveAt(i);
            }
        }
    }

    private void SyncChannels(IReadOnlyList<ChannelStatistics> channels)
    {
        var known = Channels.ToDictionary(static row => row.Name, StringComparer.Ordinal);
        var live = new HashSet<string>(StringComparer.Ordinal);
        foreach (var channel in channels)
        {
            live.Add(channel.Name);
            if (!known.TryGetValue(channel.Name, out var row))
            {
                row = new ChannelRow { Name = channel.Name };
                Channels.Add(row);
            }

            row.Kind = channel.Kind.ToString();
            row.Owner = string.IsNullOrEmpty(channel.Owner) ? "—" : channel.Owner[..Math.Min(8, channel.Owner.Length)];
            row.Count = $"{channel.QueuedMessages} / {channel.MaxMessages}";
            row.Bytes = channel.MaxBytes is long cap ? $"{Format.Bytes(channel.QueuedBytes)} / {Format.Bytes(cap)}" : Format.Bytes(channel.QueuedBytes);
            row.FillPercent = channel.FillPercent;
            row.FillBrush = channel.FillPercent >= 80 || channel.WarningActive
                ? SolidColorBrush.Parse("#F0B429")
                : SolidColorBrush.Parse("#3DDC97");
            row.Policy = channel.FullPolicy.ToString();
            row.Dropped = $"{channel.DroppedMessages} / {channel.ClearedMessages}";
        }

        for (var i = Channels.Count - 1; i >= 0; i--)
        {
            if (!live.Contains(Channels[i].Name))
            {
                Channels.RemoveAt(i);
            }
        }
    }

    private void SyncPressure()
    {
        var lines = _host.Pressure();
        if (lines.Count == _pressureSeen)
        {
            return;
        }

        for (var i = _pressureSeen; i < lines.Count; i++)
        {
            Pressure.Add(lines[i]);
        }

        _pressureSeen = lines.Count;
        while (Pressure.Count > 200)
        {
            Pressure.RemoveAt(0);
        }
    }
}

public static class Format
{
    public static string Bytes(double value)
    {
        var abs = Math.Abs(value);
        if (abs >= 1024 * 1024)
        {
            return (value / (1024 * 1024)).ToString("0.00") + " MB";
        }

        if (abs >= 1024)
        {
            return (value / 1024).ToString("0.0") + " KB";
        }

        return value.ToString("0") + " B";
    }
}
