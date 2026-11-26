using System.Net;
using DomeMQ;

namespace DomeMQ.Protocol.Tests;

public class TrafficObserverTests
{
    [Fact]
    public async Task Partial_reads_are_reported_as_separate_chunks()
    {
        var server = new CollectingObserver();
        var client = new CollectingObserver();
        await using var listener = TransportRegistry.Resolve("tcp").Listen(new TransportSettings
        {
            Host = "127.0.0.1",
            Port = 0,
            NoDelay = true,
            TrafficObserver = server
        });
        var port = Assert.IsType<IPEndPoint>(listener.LocalEndPoint).Port;
        var accept = listener.AcceptAsync(CancellationToken.None);
        await using var connected = await TransportRegistry.Resolve("tcp").ConnectAsync(new TransportSettings
        {
            Host = "127.0.0.1",
            Port = port,
            NoDelay = true,
            TrafficObserver = client
        }, CancellationToken.None);
        await using var accepted = await accept;

        var payload = Enumerable.Range(0, 50).Select(static i => (byte)i).ToArray();
        await connected.SendAsync(payload, CancellationToken.None);

        var received = 0;
        var buffer = new byte[10];
        while (received < payload.Length)
        {
            var count = await accepted.ReceiveAsync(buffer, CancellationToken.None);
            Assert.InRange(count, 1, buffer.Length);
            received += count;
        }

        var inbound = server.Chunks.Where(static chunk => chunk.Direction == TrafficDirection.Inbound).ToArray();
        Assert.True(inbound.Length >= 5);
        Assert.Equal(payload, inbound.SelectMany(static chunk => chunk.Data).ToArray());
        Assert.All(inbound, chunk => Assert.Contains("127.0.0.1", chunk.Endpoint));
        Assert.Equal(payload, client.Chunks.Where(static chunk => chunk.Direction == TrafficDirection.Outbound).SelectMany(static chunk => chunk.Data).ToArray());
        Assert.Null(accepted.PeerCertificateSubject);
        Assert.Null(connected.PeerCertificateSubject);
    }

    [Fact]
    public async Task A_throwing_observer_does_not_break_the_send()
    {
        var observer = new ThrowingObserver();
        await using var listener = TransportRegistry.Resolve("tcp").Listen(new TransportSettings
        {
            Host = "127.0.0.1",
            Port = 0,
            TrafficObserver = observer
        });
        var port = Assert.IsType<IPEndPoint>(listener.LocalEndPoint).Port;
        var accept = listener.AcceptAsync(CancellationToken.None);
        await using var connected = await TransportRegistry.Resolve("tcp").ConnectAsync(new TransportSettings
        {
            Host = "127.0.0.1",
            Port = port
        }, CancellationToken.None);
        await using var accepted = await accept;
        await connected.SendAsync(new byte[] { 1, 2, 3, 4 }, CancellationToken.None);
        var buffer = new byte[4];
        var count = await accepted.ReceiveAsync(buffer, CancellationToken.None);
        Assert.Equal(4, count);
        Assert.True(observer.Calls > 0);
    }

    private sealed class CollectingObserver : ITrafficObserver
    {
        public List<TrafficChunk> Chunks { get; } = [];

        public void OnChunk(TrafficChunk chunk) => Chunks.Add(chunk);
    }

    private sealed class ThrowingObserver : ITrafficObserver
    {
        public int Calls;

        public void OnChunk(TrafficChunk chunk)
        {
            Calls++;
            throw new InvalidOperationException("observer failed");
        }
    }
}
