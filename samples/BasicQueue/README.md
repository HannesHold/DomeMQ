# BasicQueue

Embeds `DomeMqBroker` on loopback TCP and a Unix socket, then moves one queue between those connections. The owner connects over TCP. A second connection pushes over the Unix socket. The same program round-trips an `IPayload` and an `IPayloadSerializer<int>`.

```bash
dotnet run --project samples/BasicQueue
```

The listing in [docs/examples.md](../../docs/examples.md) is this program.
