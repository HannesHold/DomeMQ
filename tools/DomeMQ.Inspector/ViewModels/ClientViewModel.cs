using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Text.Json;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using DomeMQ;
using DomeMQ.Client;
using DomeMQ.Inspector.Services;

namespace DomeMQ.Inspector.ViewModels;

public partial class ClientViewModel : ObservableObject
{
    private readonly TrafficLog _log;
    private readonly TrafficViewModel _traffic;
    private DomeMqClient? _client;
    private CancellationTokenSource? _stress;
    private CancellationTokenSource? _receive;
    private readonly ConcurrentQueue<ReceivedRow> _incoming = new();
    private readonly List<double> _latencyMs = [];
    private readonly Queue<double> _rates = new();
    private int _sent;
    private int _accepted;
    private int _discarded;
    private int _failed;
    private int _target;
    private long _latencyCount;
    private double _latencySum;
    private double _latencyMin = double.MaxValue;
    private double _latencyMax;
    private DateTimeOffset _stressStarted;
    private int _lastSentSample;
    private DateTimeOffset _lastRateSample;
    private long _receivedBytes;
    private int _receivedCount;
    private DateTimeOffset _receiveSample = DateTimeOffset.UtcNow;
    private int _receiveAtSample;

    public ClientViewModel(TrafficLog log, TrafficViewModel traffic)
    {
        _log = log;
        _traffic = traffic;
        Received = [];
        Scenarios = new ObservableCollection<ScenarioRow>(ScenarioCatalog.All.Select(item => new ScenarioRow(item, this)));
        StressRates = [];
        ReceiveRates = [];
    }

    public string[] Transports { get; } = ["tcp", "unix"];

    public string[] ChannelTypes { get; } = ["Queue", "Stack", "Bag"];

    public string[] Policies { get; } = ["Inherit", "Reject", "DiscardNewest", "DiscardOldest", "ClearAll"];

    public string[] PayloadModes { get; } = ["Text", "Hex"];

    public string[] Views { get; } = ["Text", "Hex", "JSON"];

    public ObservableCollection<ReceivedRow> Received { get; }

    public ObservableCollection<ScenarioRow> Scenarios { get; }

    public ObservableCollection<HexRow> MessageRows { get; } = [];

    [ObservableProperty] private string _transport = "tcp";
    [ObservableProperty] private string _host = "127.0.0.1";
    [ObservableProperty] private int _port = 5071;
    [ObservableProperty] private string _socketPath = "";
    [ObservableProperty] private string _clientName = "inspector";
    [ObservableProperty] private string _authToken = "";
    [ObservableProperty] private bool _tlsEnabled;
    [ObservableProperty] private bool _allowUntrusted = true;
    [ObservableProperty] private string _targetHost = "";
    [ObservableProperty] private string _pinSpki = "";
    [ObservableProperty] private string _pinCertificatePath = "";
    [ObservableProperty] private string _pfxPath = "";
    [ObservableProperty] private string _pfxPassword = "";
    [ObservableProperty] private string _certificatePath = "";
    [ObservableProperty] private string _keyPath = "";
    [ObservableProperty] private string _status = "Disconnected";
    [ObservableProperty] private bool _connected;

    [ObservableProperty] private string _channelName = "demo";
    [ObservableProperty] private string _channelType = "Queue";
    [ObservableProperty] private string _maxMessages = "";
    [ObservableProperty] private string _maxPayload = "";
    [ObservableProperty] private string _maxBytes = "";
    [ObservableProperty] private string _fullPolicy = "Inherit";
    [ObservableProperty] private string _channelStatus = "";

    [ObservableProperty] private string _payload = "hello from DomeMQ Inspector";
    [ObservableProperty] private string _payloadMode = "Text";
    [ObservableProperty] private string _targetChannel = "demo";
    [ObservableProperty] private int _sendCount = 1000;
    [ObservableProperty] private int _parallelism = 1;
    [ObservableProperty] private int _intervalMs;
    [ObservableProperty] private bool _stressRunning;
    [ObservableProperty] private double _progress;
    [ObservableProperty] private string _stressResult = "Idle";
    public double[] StressRates { get; private set; }

    [ObservableProperty] private string _receiveChannel = "demo";
    [ObservableProperty] private bool _autoReceive;
    [ObservableProperty] private string _receiveStats = "0 msgs/s   0 MB/s   total 0";
    [ObservableProperty] private ReceivedRow? _selectedMessage;
    [ObservableProperty] private string _messageView = "Text";
    [ObservableProperty] private string _messageText = "";
    [ObservableProperty] private string _exportPath = "";
    public double[] ReceiveRates { get; private set; }
    private readonly Queue<double> _receiveRates = new();

    [ObservableProperty] private bool _testsRunning;

    public event Action? ChartUpdated;

    public event Action? ShowLogRequested;

    public bool ShowHex => MessageView == "Hex";

    public bool ShowText => MessageView != "Hex";

    [RelayCommand]
    private async Task Connect()
    {
        try
        {
            Status = "Connecting…";
            var options = BuildOptions();
            _client = await DomeMqClient.ConnectAsync(options);
            Connected = true;
            Status = $"Connected via {Transport} as {ClientName}";
        }
        catch (Exception ex)
        {
            Connected = false;
            Status = ex.Message;
        }
    }

    [RelayCommand]
    private async Task Disconnect()
    {
        await StopStress();
        await StopReceive();
        if (_client is not null)
        {
            await _client.DisposeAsync();
            _client = null;
        }

        Connected = false;
        Status = "Disconnected";
    }

    [RelayCommand]
    private async Task CreateChannel()
    {
        if (_client is null)
        {
            ChannelStatus = "Connect first.";
            return;
        }

        try
        {
            var info = await _client.CreateChannelAsync(ChannelName, ParseKind(ChannelType), new CreateChannelOptions
            {
                MaxQueueLength = ParseOptional(MaxMessages),
                MaxPayloadSize = ParseOptional(MaxPayload),
                MaxBytes = ParseOptional(MaxBytes),
                FullPolicy = ParsePolicy(FullPolicy)
            });
            TargetChannel = info.Name;
            ReceiveChannel = info.Name;
            ChannelStatus = $"{info.Name} {info.Kind} max {info.MaxQueueLength} payload {info.MaxPayloadSize} policy {info.FullPolicy} bytes {info.MaxBytes}";
        }
        catch (Exception ex)
        {
            ChannelStatus = ex.Message;
        }
    }

    [RelayCommand]
    private async Task DeleteChannel()
    {
        if (_client is null)
        {
            ChannelStatus = "Connect first.";
            return;
        }

        try
        {
            await _client.DeleteChannelAsync(ChannelName);
            ChannelStatus = "Deleted " + ChannelName;
        }
        catch (Exception ex)
        {
            ChannelStatus = ex.Message;
        }
    }

    [RelayCommand]
    private async Task StartStress()
    {
        if (_client is null)
        {
            StressResult = "Connect first.";
            return;
        }

        byte[] body;
        try
        {
            body = HexFormat.ParsePayload(Payload, PayloadMode == "Hex");
        }
        catch (Exception ex)
        {
            StressResult = ex.Message;
            return;
        }

        await StopStress();
        _sent = _accepted = _discarded = _failed = 0;
        _target = Math.Max(1, SendCount);
        _latencyCount = 0;
        _latencySum = 0;
        _latencyMin = double.MaxValue;
        _latencyMax = 0;
        lock (_latencyMs)
        {
            _latencyMs.Clear();
        }

        _rates.Clear();
        _stressStarted = DateTimeOffset.UtcNow;
        _lastRateSample = _stressStarted;
        _lastSentSample = 0;
        StressRunning = true;
        Progress = 0;
        var cts = new CancellationTokenSource();
        _stress = cts;
        var client = _client;
        var channel = TargetChannel;
        var parallel = Math.Max(1, Parallelism);
        var interval = Math.Max(0, IntervalMs);
        _ = Task.Run(async () =>
        {
            try
            {
                await Parallel.ForEachAsync(
                    Enumerable.Range(0, _target),
                    new ParallelOptions { MaxDegreeOfParallelism = parallel, CancellationToken = cts.Token },
                    async (_, token) =>
                    {
                        if (interval > 0)
                        {
                            await Task.Delay(interval, token).ConfigureAwait(false);
                        }

                        var watch = Stopwatch.StartNew();
                        try
                        {
                            var result = await client.PushAsync(channel, body, cancellationToken: token).ConfigureAwait(false);
                            watch.Stop();
                            NoteLatency(watch.Elapsed.TotalMilliseconds);
                            if (result.Discarded)
                            {
                                Interlocked.Increment(ref _discarded);
                            }
                            else
                            {
                                Interlocked.Increment(ref _accepted);
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            throw;
                        }
                        catch
                        {
                            Interlocked.Increment(ref _failed);
                        }

                        Interlocked.Increment(ref _sent);
                    });
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                Dispatcher.UIThread.Post(() =>
                {
                    StressRunning = false;
                    PublishStress(final: true);
                });
            }
        });
    }

    [RelayCommand]
    private async Task StopStress()
    {
        if (_stress is not null)
        {
            await _stress.CancelAsync();
            _stress.Dispose();
            _stress = null;
        }

        StressRunning = false;
    }

    [RelayCommand]
    private async Task PopOnce()
    {
        if (_client is null)
        {
            ReceiveStats = "Connect first.";
            return;
        }

        try
        {
            var messages = await _client.PopAsync(ReceiveChannel, new PopOptions { Timeout = TimeSpan.FromSeconds(2) });
            Accept(messages, ReceiveChannel);
        }
        catch (Exception ex)
        {
            ReceiveStats = ex.Message;
        }
    }

    partial void OnAutoReceiveChanged(bool value)
    {
        if (value)
        {
            _ = RunReceive();
        }
        else
        {
            _ = StopReceive();
        }
    }

    [RelayCommand]
    private void ClearReceived()
    {
        Received.Clear();
        _receivedBytes = 0;
        _receivedCount = 0;
        SelectedMessage = null;
        ReceiveStats = "0 msgs/s   0 MB/s   total 0";
    }

    [RelayCommand]
    private void ExportReceived()
    {
        var path = string.IsNullOrWhiteSpace(ExportPath)
            ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "domemq-messages.txt")
            : ExportPath;
        WriteReceived(path);
    }

    public void WriteReceived(string path)
    {
        using var writer = new StreamWriter(path);
        foreach (var row in Received)
        {
            writer.WriteLine($"{row.Timestamp:O} {row.Channel} {row.Size} bytes req {row.RequestId}");
            writer.WriteLine(row.Text);
            writer.WriteLine();
        }

        ExportPath = path;
        ReceiveStats = "Exported " + path;
    }

    [RelayCommand]
    private async Task RunAll()
    {
        if (TestsRunning)
        {
            return;
        }

        TestsRunning = true;
        try
        {
            foreach (var row in Scenarios)
            {
                await RunScenario(row);
            }
        }
        finally
        {
            TestsRunning = false;
        }
    }

    public async Task RunScenario(ScenarioRow row)
    {
        if (_client is null && row.Name is not "Whitelist reject" and not "TLS" and not "mTLS")
        {
            row.State = "Fail";
            row.Detail = "Connect first.";
            return;
        }

        row.State = "Running";
        row.Detail = "";
        var outcome = await row.Definition.Run(Context(), CancellationToken.None);
        row.State = outcome.Passed ? "Pass" : "Fail";
        row.Duration = outcome.Duration.TotalMilliseconds.ToString("0") + " ms";
        row.Detail = outcome.Detail;
        row.LogFrom = outcome.LogFrom;
        row.LogTo = outcome.LogTo;
    }

    public void ShowFrames(ScenarioRow row)
    {
        _traffic.ShowRange(row.LogFrom, row.LogTo);
        ShowLogRequested?.Invoke();
    }

    public void Refresh()
    {
        while (_incoming.TryDequeue(out var row))
        {
            Received.Add(row);
            while (Received.Count > 500)
            {
                Received.RemoveAt(0);
            }
        }

        if (StressRunning || _sent > 0)
        {
            PublishStress(final: false);
        }

        SampleReceive();
    }

    partial void OnSelectedMessageChanged(ReceivedRow? value) => RenderMessage();

    partial void OnMessageViewChanged(string value)
    {
        OnPropertyChanged(nameof(ShowHex));
        OnPropertyChanged(nameof(ShowText));
        RenderMessage();
    }

    private void PublishStress(bool final)
    {
        var sent = Volatile.Read(ref _sent);
        var accepted = Volatile.Read(ref _accepted);
        var discarded = Volatile.Read(ref _discarded);
        var failed = Volatile.Read(ref _failed);
        Progress = _target == 0 ? 0 : Math.Min(100, sent * 100.0 / _target);
        var elapsed = DateTimeOffset.UtcNow - _stressStarted;
        var seconds = Math.Max(0.001, elapsed.TotalSeconds);
        var mb = accepted * (double)Math.Max(1, PayloadBytes()) / (1024 * 1024);
        double min, max, avg, p99;
        lock (_latencyMs)
        {
            min = _latencyCount == 0 ? 0 : _latencyMin;
            max = _latencyMax;
            avg = _latencyCount == 0 ? 0 : _latencySum / _latencyCount;
            p99 = Percentile();
        }

        StressResult = $"{sent}/{_target} sent   {accepted} accepted   {discarded} discarded   {failed} failed   {elapsed.TotalSeconds:0.00}s   {accepted / seconds:0.0} msgs/s   {mb / seconds:0.00} MB/s   latency ms min {min:0.00} avg {avg:0.00} max {max:0.00} p99 {p99:0.00}";
        if (final)
        {
            StressRunning = false;
        }

        var now = DateTimeOffset.UtcNow;
        var window = (now - _lastRateSample).TotalSeconds;
        if (window >= 0.25 || final)
        {
            var rate = (sent - _lastSentSample) / Math.Max(window, 0.001);
            _lastSentSample = sent;
            _lastRateSample = now;
            _rates.Enqueue(rate);
            if (_rates.Count == 1)
            {
                _rates.Enqueue(rate);
            }

            while (_rates.Count > 60)
            {
                _rates.Dequeue();
            }

            StressRates = _rates.ToArray();
            ChartUpdated?.Invoke();
        }
    }

    private void SampleReceive()
    {
        var now = DateTimeOffset.UtcNow;
        var seconds = (now - _receiveSample).TotalSeconds;
        if (seconds < 0.4)
        {
            return;
        }

        var count = _receivedCount;
        var rate = (count - _receiveAtSample) / seconds;
        _receiveAtSample = count;
        _receiveSample = now;
        var mb = _receivedBytes / (1024d * 1024d);
        ReceiveStats = $"{rate:0.0} msgs/s   {(rate * (_receivedCount == 0 ? 0 : mb / _receivedCount)):0.000} MB/s   total {_receivedCount}  {Format.Bytes(_receivedBytes)}";
        _receiveRates.Enqueue(rate);
        while (_receiveRates.Count > 60)
        {
            _receiveRates.Dequeue();
        }

        ReceiveRates = _receiveRates.ToArray();
        ChartUpdated?.Invoke();
    }

    private async Task RunReceive()
    {
        await StopReceive();
        if (_client is null)
        {
            AutoReceive = false;
            ReceiveStats = "Connect first.";
            return;
        }

        var cts = new CancellationTokenSource();
        _receive = cts;
        var client = _client;
        var channel = ReceiveChannel;
        _ = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    var messages = await client.PopAsync(channel, new PopOptions { Timeout = TimeSpan.FromMilliseconds(200) }, cts.Token).ConfigureAwait(false);
                    Accept(messages, channel);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Dispatcher.UIThread.Post(() => ReceiveStats = ex.Message);
                    break;
                }
            }
        });
    }

    private async Task StopReceive()
    {
        if (_receive is null)
        {
            return;
        }

        await _receive.CancelAsync();
        _receive.Dispose();
        _receive = null;
    }

    private void Accept(IReadOnlyList<DomeMqMessage> messages, string channel)
    {
        foreach (var message in messages)
        {
            var row = ReceivedRow.From(message, channel);
            _incoming.Enqueue(row);
            Interlocked.Increment(ref _receivedCount);
            Interlocked.Add(ref _receivedBytes, message.Body.Length);
        }
    }

    private void RenderMessage()
    {
        MessageRows.Clear();
        if (SelectedMessage is null)
        {
            MessageText = "";
            return;
        }

        if (MessageView == "Hex")
        {
            foreach (var row in HexFormat.Rows(SelectedMessage.Body, 0))
            {
                MessageRows.Add(row);
            }

            MessageText = "";
            return;
        }

        if (MessageView == "JSON")
        {
            try
            {
                using var document = JsonDocument.Parse(SelectedMessage.Text);
                MessageText = JsonSerializer.Serialize(document, new JsonSerializerOptions { WriteIndented = true });
            }
            catch (Exception)
            {
                MessageText = "Not JSON.";
            }

            return;
        }

        MessageText = SelectedMessage.Text;
    }

    private void NoteLatency(double ms)
    {
        lock (_latencyMs)
        {
            _latencyCount++;
            _latencySum += ms;
            if (ms < _latencyMin)
            {
                _latencyMin = ms;
            }

            if (ms > _latencyMax)
            {
                _latencyMax = ms;
            }

            if (_latencyMs.Count < 50_000)
            {
                _latencyMs.Add(ms);
            }
        }
    }

    private double Percentile()
    {
        if (_latencyMs.Count == 0)
        {
            return 0;
        }

        var copy = _latencyMs.ToArray();
        Array.Sort(copy);
        var index = (int)Math.Clamp(Math.Ceiling(copy.Length * 0.99) - 1, 0, copy.Length - 1);
        return copy[index];
    }

    private int PayloadBytes()
    {
        try
        {
            return HexFormat.ParsePayload(Payload, PayloadMode == "Hex").Length;
        }
        catch (Exception)
        {
            return 0;
        }
    }

    private ScenarioContext Context() => new()
    {
        Client = _client,
        Options = BuildOptions(),
        Log = _log,
        TlsEnabled = TlsEnabled,
        ClientCertificateConfigured = TlsEnabled && (!string.IsNullOrWhiteSpace(PfxPath) || !string.IsNullOrWhiteSpace(CertificatePath))
    };

    private DomeMqClientOptions BuildOptions()
    {
        var unix = Transport == "unix";
        return new DomeMqClientOptions
        {
            Transport = unix ? "unixsocket" : "tcp",
            Host = Host,
            Port = Port,
            Path = string.IsNullOrWhiteSpace(SocketPath) ? null : SocketPath,
            ClientName = string.IsNullOrWhiteSpace(ClientName) ? "inspector" : ClientName,
            AuthToken = string.IsNullOrWhiteSpace(AuthToken) ? null : AuthToken,
            HeartbeatInterval = TimeSpan.FromSeconds(30),
            PopTimeout = TimeSpan.FromSeconds(5),
            TrafficObserver = _log,
            Tls = TlsEnabled ? new TlsClientOptions
            {
                Enabled = true,
                AllowUntrustedCertificates = AllowUntrusted,
                ServerName = string.IsNullOrWhiteSpace(TargetHost) ? null : TargetHost,
                Pin = string.IsNullOrWhiteSpace(PinSpki) && string.IsNullOrWhiteSpace(PinCertificatePath)
                    ? null
                    : new ServerPinOptions
                    {
                        SpkiSha256 = string.IsNullOrWhiteSpace(PinSpki) ? null : PinSpki,
                        CertificateFile = string.IsNullOrWhiteSpace(PinCertificatePath) ? null : PinCertificatePath
                    },
                PfxPath = string.IsNullOrWhiteSpace(PfxPath) ? null : PfxPath,
                PfxPassword = PfxPassword,
                CertificatePath = string.IsNullOrWhiteSpace(CertificatePath) ? null : CertificatePath,
                KeyPath = string.IsNullOrWhiteSpace(KeyPath) ? null : KeyPath
            } : null
        };
    }

    private static ChannelKind ParseKind(string value) => value switch
    {
        "Stack" => ChannelKind.Stack,
        "Bag" => ChannelKind.Bag,
        _ => ChannelKind.Queue
    };

    private static global::DomeMQ.FullPolicy? ParsePolicy(string value) => value switch
    {
        "Reject" => global::DomeMQ.FullPolicy.Reject,
        "DiscardNewest" => global::DomeMQ.FullPolicy.DiscardNewest,
        "DiscardOldest" => global::DomeMQ.FullPolicy.DiscardOldest,
        "ClearAll" => global::DomeMQ.FullPolicy.ClearAll,
        _ => null
    };

    private static int? ParseOptional(string text) =>
        int.TryParse(text, out var value) && value > 0 ? value : null;
}

public partial class ReceivedRow : ObservableObject
{
    public DateTimeOffset Timestamp { get; init; }

    public string Channel { get; init; } = "";

    public int Size { get; init; }

    public uint RequestId { get; init; }

    public byte[] Body { get; init; } = [];

    public string Text { get; init; } = "";

    public string TimeLabel => Timestamp.ToLocalTime().ToString("HH:mm:ss.fff");

    public static ReceivedRow From(DomeMqMessage message, string channel) => new()
    {
        Timestamp = DateTimeOffset.UtcNow,
        Channel = channel,
        Size = message.Body.Length,
        RequestId = message.RequestId,
        Body = message.Body,
        Text = System.Text.Encoding.UTF8.GetString(message.Body)
    };
}

public partial class ScenarioRow : ObservableObject
{
    private readonly ClientViewModel _owner;

    public ScenarioRow(ScenarioDefinition definition, ClientViewModel owner)
    {
        Definition = definition;
        _owner = owner;
        Name = definition.Name;
        Explanation = definition.Explanation;
        State = "—";
    }

    public ScenarioDefinition Definition { get; }

    public string Name { get; }

    public string Explanation { get; }

    public long LogFrom { get; set; }

    public long LogTo { get; set; }

    [ObservableProperty] private string _state;
    [ObservableProperty] private string _duration = "";
    [ObservableProperty] private string _detail = "";

    [RelayCommand]
    private Task Run() => _owner.RunScenario(this);

    [RelayCommand]
    private void Show() => _owner.ShowFrames(this);
}
