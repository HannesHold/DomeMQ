# DomeMQ

DomeMQ is a small, cross-platform IPC message broker for .NET. Processes connect over TCP or Unix domain sockets, create their own channels (`Queue`, `Stack`, or `Bag`), and exchange opaque byte payloads. The wire protocol is documented and has no third-party serializer, so a client can be written in any language.

The broker runs embedded in a process, as a standalone executable, or as a Linux container. `DomeMQ.Client` is the .NET client.

## Features

- TCP and Unix domain sockets on Linux and Windows 10 or later, through the same socket API.
- Extension points for UDP, KCP, and shared memory. Those factories are registered and throw until an implementation is plugged in.
- Channels owned by the connection that created them. Anyone can push. Only the owner can pop or delete.
- Fixed 16-byte little-endian header and a custom binary body. Application payloads are raw bytes.
- Optional TLS on TCP. Server keys are named pairs loaded from an environment variable or a file, and can be rotated without a restart. Local transports run unencrypted.
- Payload limits at server, listener, channel, and per-call scope. The effective limit is the minimum of the levels that are set.
- Queue overflow policies: reject, discard the newest, discard the oldest, or clear the channel. Message, byte, per-client, and server memory caps are separate. Crossing 80% raises a pressure event.
- IP allow lists and block lists, including CIDR ranges, with a per-listener override and a periodic reload.
- Configuration in `appsettings.json` or `domemq.json`, overridden by environment variables and `--config`.

## Requirements

Building requires the .NET 10 SDK. The shipped packages target `net10.0`.

## Layout

```text
DomeMQ.sln
src/DomeMQ.Protocol    frame codec, messages, transports (no NuGet dependencies)
src/DomeMQ.Client      client library
src/DomeMQ.Server      embeddable broker
src/DomeMQ.Broker      standalone host
tests/                 xUnit
docs/protocol.md       language-independent protocol
docs/configuration.md  every broker option and its default
```

## Quick start

### Standalone executable

Release tags `v*` publish self-contained single-file builds for `win-x64`, `linux-x64`, and `linux-arm64`. Each zip contains the binary and a sample `appsettings.json`.

```bash
./DomeMQ.Broker
# or, on Windows:
DomeMQ.Broker.exe
```

The broker listens on `0.0.0.0:5071` with the sample file. Edit that file, or point at another one:

```bash
./DomeMQ.Broker --config /etc/domemq.json
```

### Docker

The image listens on port 5071 and reads `/config/domemq.json` or `/config/appsettings.json` when you mount one. Environment variables override the file.

```bash
docker build -t domemq .
docker run --rm -p 5071:5071 \
  -v "$PWD/src/DomeMQ.Broker/appsettings.json:/config/domemq.json:ro" \
  domemq
```

Published images are tagged with the release version:

```text
ghcr.io/hanneshold/domemq:0.1.0
```

### NuGet

`DomeMQ.Protocol`, `DomeMQ.Client`, and `DomeMQ.Server` are packed with the repository version. They depend only on the shared framework (and, for Client and Server, on `DomeMQ.Protocol`). The broker host is the only project that references `Microsoft.Extensions.*`.

```bash
dotnet add package DomeMQ.Client
dotnet add package DomeMQ.Server
```

### Embed the broker

```csharp
using DomeMQ.Server;

var options = DomeMqOptions.CreateDefault();
options.Listeners[0].Host = "127.0.0.1";
options.Listeners[0].Port = 5071;

await using var broker = new DomeMqBroker(options);
await broker.StartAsync();
```

## Client

```csharp
using DomeMQ;
using DomeMQ.Client;

await using var client = await DomeMqClient.ConnectAsync(new DomeMqClientOptions
{
    Transport = "tcp",
    Host = "127.0.0.1",
    Port = 5071
});

await client.CreateChannelAsync("orders", ChannelKind.Queue);
await client.PushAsync("orders", "hello"u8.ToArray());

var messages = await client.PopAsync("orders", new PopOptions
{
    MaxMessages = 16,
    Timeout = TimeSpan.FromSeconds(1)
});
```

Unix socket:

```csharp
await using var client = await DomeMqClient.ConnectAsync(new DomeMqClientOptions
{
    Transport = "unix",
    Path = "/tmp/domemq.sock"
});
```

`PushAsync` and `PopAsync` also accept `IPayload` values (`RawPayload`, `Utf8StringPayload`, `JsonPayload<T>`) and an `IPayloadSerializer<T>` you implement yourself. The bytes on the wire are still a length-prefixed blob. See below.

Per-call limits inherit the server limit unless you set them:

```csharp
await client.PushAsync("orders", payload, new PushOptions { MaxPayloadSize = 4096 });
var batch = await client.PopAsync("orders", new PopOptions { MaxPayloadSize = 4096 });
```

A body over the effective limit throws `PayloadTooLargeException` on the client and is rejected by the server with error code 8.

## Bring your own serializer

DomeMQ does not reference MessagePack, Protobuf, or JSON libraries beyond `System.Text.Json` in the optional `JsonPayload<T>` helper. Implement one of these in your own project.

`IPayload` for a type the client constructs with `new()`:

```csharp
public sealed class MyTextPayload : IPayload
{
    public string Text { get; set; } = "";
    public int? SizeHint => Encoding.UTF8.GetByteCount(Text);
    public ReadOnlyMemory<byte> GetPayload() => Encoding.UTF8.GetBytes(Text);
    public void SetPayload(ReadOnlySpan<byte> data) => Text = Encoding.UTF8.GetString(data);
}

await client.PushAsync("orders", new MyTextPayload { Text = "hello" });
var items = await client.PopAsync<MyTextPayload>("orders");
```

`IWritablePayload` is optional. If the object implements it, the client writes through `IBufferWriter<byte>` instead of `GetPayload`.

`IPayloadSerializer<T>` adapts a serializer you already have. The example writes a big-endian `int`. A MessagePack or Protobuf library would take the same shape inside `Serialize` / `Deserialize`.

```csharp
public sealed class Int32BeSerializer : IPayloadSerializer<int>
{
    public void Serialize(int value, IBufferWriter<byte> destination)
    {
        var span = destination.GetSpan(4);
        BinaryPrimitives.WriteInt32BigEndian(span, value);
        destination.Advance(4);
    }

    public int Deserialize(ReadOnlySpan<byte> source) =>
        BinaryPrimitives.ReadInt32BigEndian(source);
}

await client.PushAsync("orders", 42, new Int32BeSerializer());
IReadOnlyList<int> values = await client.PopAsync("orders", new Int32BeSerializer());
```

`PushOptions.ContentType` is stored with the bytes and returned on `DomeMqMessage.ContentType`. The broker does not interpret it. Use it as a hint for your own decoder (`application/json`, `application/x-msgpack`, and so on).

## Protocol

The byte layout, message fields, error codes, connection flow, versioning rules, and hex test vectors are in [docs/protocol.md](docs/protocol.md).

## Configuration

Every broker option, its default, and the environment-variable form are in [docs/configuration.md](docs/configuration.md). The commented sample is [src/DomeMQ.Broker/appsettings.json](src/DomeMQ.Broker/appsettings.json).

Payload limits default to 16 MiB globally. Listener, channel, and per-message limits default to unset, which means "use the server limit". Other limits are intentionally roomy: 1024 clients, 10000 channels, 100000 messages per channel, 256 KiB socket buffers.

## Build and test

```bash
dotnet test DomeMQ.sln -c Release
```

## Versioning and releases

The repository version in `Directory.Build.props` is `0.1.0`. A tag `v*` (for example `v0.1.0`) builds that same version into:

- the container `ghcr.io/<owner>/<repo>:<version>` (lowercase)
- the NuGet packages, when the `NUGET_API_KEY` secret is present (the job skips publish and succeeds when the secret is missing)
- a GitHub Release containing the self-contained executables

## License

MIT. See [LICENSE](LICENSE). Copyright (c) 2026 Hannes Hold.
