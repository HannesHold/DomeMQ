using DomeMQ;
using DomeMQ.Server;

namespace DomeMQ.Inspector.Services;

public sealed class ClientSnapshot
{
    public required string Id { get; init; }

    public required string Endpoint { get; init; }

    public required DateTimeOffset ConnectedAt { get; init; }

    public required string Certificate { get; init; }

    public required string KeyId { get; init; }

    public required bool Connected { get; init; }
}

public sealed class ServerHost : IBrokerConnectionObserver
{
    private readonly object _gate = new();
    private readonly List<ClientSnapshot> _clients = [];
    private readonly List<string> _pressure = [];
    private DomeMqBroker? _broker;

    public bool Running => _broker is not null;

    public string Status { get; private set; } = "Stopped";

    public string ConfigSummary { get; private set; } = "Defaults: TCP 0.0.0.0:5071, TLS off, IP rules off.";

    public long MaxQueuedBytes { get; private set; } = DomeMqDefaults.MaxQueuedBytes;

    public async Task StartAsync(string? configPath, ITrafficObserver traffic, CancellationToken cancellationToken)
    {
        if (_broker is not null)
        {
            return;
        }

        var options = ConfigLoader.Load(configPath);
        var broker = new DomeMqBroker(options)
        {
            ConnectionObserver = this,
            TrafficObserver = traffic
        };
        broker.QueuePressure += OnPressure;
        await broker.StartAsync(cancellationToken).ConfigureAwait(false);
        _broker = broker;
        MaxQueuedBytes = options.Limits.MaxQueuedBytes;
        ConfigSummary = ConfigLoader.Summarize(options);
        Status = "Running";
    }

    public async Task StopAsync()
    {
        var broker = _broker;
        _broker = null;
        if (broker is null)
        {
            return;
        }

        broker.QueuePressure -= OnPressure;
        await broker.DisposeAsync().ConfigureAwait(false);
        lock (_gate)
        {
            _clients.Clear();
        }

        Status = "Stopped";
    }

    public BrokerStatistics? Statistics() => _broker?.GetStatistics();

    public List<ClientSnapshot> Clients()
    {
        lock (_gate)
        {
            return _clients.ToList();
        }
    }

    public List<string> Pressure()
    {
        lock (_gate)
        {
            return _pressure.ToList();
        }
    }

    public void OnConnected(BrokerConnectionSnapshot snapshot)
    {
        lock (_gate)
        {
            _clients.RemoveAll(item => item.Id == snapshot.Id);
            _clients.Add(new ClientSnapshot
            {
                Id = snapshot.Id,
                Endpoint = snapshot.Remote,
                ConnectedAt = snapshot.ConnectedAt,
                Certificate = snapshot.ClientCertificateSubject ?? "",
                KeyId = snapshot.KeyId ?? "",
                Connected = true
            });
        }
    }

    public void OnDisconnected(string id)
    {
        lock (_gate)
        {
            var index = _clients.FindIndex(item => item.Id == id);
            if (index < 0)
            {
                return;
            }

            var current = _clients[index];
            _clients[index] = new ClientSnapshot
            {
                Id = current.Id,
                Endpoint = current.Endpoint,
                ConnectedAt = current.ConnectedAt,
                Certificate = current.Certificate,
                KeyId = current.KeyId,
                Connected = false
            };
        }
    }

    private void OnPressure(object? sender, QueuePressure notice)
    {
        var line = $"{DateTimeOffset.Now:HH:mm:ss.fff}  {notice.Scope}  {notice.FillPercent}%  queued {notice.QueuedMessages}  {notice.QueuedBytes} bytes  dropped {notice.DroppedMessages}  cleared {notice.ClearedMessages}";
        lock (_gate)
        {
            _pressure.Add(line);
            if (_pressure.Count > 200)
            {
                _pressure.RemoveAt(0);
            }
        }
    }
}
