using System.Net;
using DomeMQ.Client;
using DomeMQ.Server;

namespace DomeMQ.Server.Tests;

public class TrafficHookTests
{
    [Fact]
    public async Task Connection_observer_reports_connect_disconnect_and_channel_owner()
    {
        var options = DomeMqOptions.CreateDefault();
        options.Listeners[0].Host = "127.0.0.1";
        options.Listeners[0].Port = 0;
        options.Timeouts.HeartbeatInterval = TimeSpan.Zero;
        options.Timeouts.IdleTimeout = TimeSpan.FromMinutes(1);
        options.Channels.OrphanChannelTtl = TimeSpan.FromMinutes(5);
        options.Channels.CleanupInterval = TimeSpan.FromSeconds(30);

        var connections = new RecordingConnections();
        await using var broker = new DomeMqBroker(options)
        {
            ConnectionObserver = connections
        };
        await broker.StartAsync();
        var endpoint = Assert.IsType<IPEndPoint>(broker.Listeners[0].LocalEndPoint);

        var client = await DomeMqClient.ConnectAsync(new DomeMqClientOptions
        {
            Host = "127.0.0.1",
            Port = endpoint.Port,
            HeartbeatInterval = TimeSpan.Zero
        });
        await client.CreateChannelAsync("owned", ChannelKind.Queue);
        await client.DisposeAsync();
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (connections.Disconnected.Count == 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20);
        }

        var connected = Assert.Single(connections.Connected);
        Assert.Contains("127.0.0.1", connected.Remote);
        Assert.False(string.IsNullOrWhiteSpace(connected.Id));
        Assert.Null(connected.ClientCertificateSubject);
        Assert.Contains(connected.Id, connections.Disconnected);
        var channel = Assert.Single(broker.GetStatistics().Channels, item => item.Name == "owned");
        Assert.Equal(connected.Id, channel.Owner);
    }

    [Fact]
    public async Task Client_traffic_observer_sees_the_handshake()
    {
        var options = DomeMqOptions.CreateDefault();
        options.Listeners[0].Host = "127.0.0.1";
        options.Listeners[0].Port = 0;
        options.Timeouts.HeartbeatInterval = TimeSpan.Zero;
        options.Channels.OrphanChannelTtl = TimeSpan.Zero;
        options.Channels.CleanupInterval = TimeSpan.Zero;
        await using var broker = new DomeMqBroker(options);
        await broker.StartAsync();
        var endpoint = Assert.IsType<IPEndPoint>(broker.Listeners[0].LocalEndPoint);
        var observer = new CollectingObserver();
        await using var client = await DomeMqClient.ConnectAsync(new DomeMqClientOptions
        {
            Host = "127.0.0.1",
            Port = endpoint.Port,
            HeartbeatInterval = TimeSpan.Zero,
            TrafficObserver = observer
        });
        Assert.Contains(observer.Chunks, chunk => chunk.Direction == TrafficDirection.Outbound && chunk.Data.Length > 0);
        Assert.Contains(observer.Chunks, chunk => chunk.Direction == TrafficDirection.Inbound && chunk.Data.Length > 0);
    }

    private sealed class RecordingConnections : IBrokerConnectionObserver
    {
        public List<BrokerConnectionSnapshot> Connected { get; } = [];

        public List<string> Disconnected { get; } = [];

        public void OnConnected(BrokerConnectionSnapshot snapshot) => Connected.Add(snapshot);

        public void OnDisconnected(string id) => Disconnected.Add(id);
    }

    private sealed class CollectingObserver : ITrafficObserver
    {
        public List<TrafficChunk> Chunks { get; } = [];

        public void OnChunk(TrafficChunk chunk) => Chunks.Add(chunk);
    }
}
