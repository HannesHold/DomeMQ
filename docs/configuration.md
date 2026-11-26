# DomeMQ configuration

The standalone broker reads a JSON file and binds it to `DomeMqOptions` (`DomeMQ.Server`). Validation runs before the process starts listening. The first invalid value prints every error and exits with code 1.

## Files and precedence

Later sources replace earlier ones.

1. `appsettings.json` next to the executable.
2. `domemq.json` next to the executable, if present.
3. `/config/appsettings.json`, if present (Docker).
4. `/config/domemq.json`, if present (Docker).
5. `--config <path>` or the `DOMEMQ_CONFIG` environment variable.
6. Environment variables.
7. Command-line configuration keys.

For a single-file executable, "next to the executable" is the directory of the real process, not the extraction directory. `dotnet DomeMQ.Broker.dll` uses the directory that contains the dll.

JSON comments and trailing commas are allowed. Unknown keys under `DomeMQ` fail startup (`ErrorOnUnknownConfiguration`).

Environment variables use `__` as the nesting separator:

```text
DomeMQ__Limits__MaxClients=2048
DomeMQ__Listeners__0__Port=6000
DomeMQ__Authentication__Tokens__0=secret
DomeMQ__IpAccess__Entries__0=10.0.0.0/8
DOMEMQ_TLS_KEY=-----BEGIN PRIVATE KEY-----\n...
```

`DOMEMQ_TLS_KEY` is the variable *named* by a key source (`{ "Env": "DOMEMQ_TLS_KEY" }`). It is not a `DomeMQ__` configuration key. Its value is the PEM or base64 PKCS#12 itself.

Names of configuration keys are case-insensitive.

IP access lists and TLS key material are re-read about every 5 seconds. New connections use the new certificates. Connections that already finished the TLS handshake keep the certificate they negotiated. A reload that does not parse keeps the previous IP lists and keys and logs the error. Enabling TLS on a listener that was started without it, or changing a bind address, still requires a restart.

The shipped sample is `src/DomeMQ.Broker/appsettings.json`. It sets every documented default explicitly.

## Payload limit precedence

```text
effective = min(server global, listener, channel, message)
```

Levels left null (JSON / client API) or `0` (on the wire for channel and message limits) are skipped. The global limit is always present.

| Level | JSON | Default | Rule |
| --- | --- | --- | --- |
| Global | `Limits.MaxPayloadSize` | 16777216 (16 MiB) | Hard ceiling. 1 .. 268435456 (256 MiB). |
| Listener | `Listeners[].MaxPayloadSize` | null (inherit) | 1 .. global. Applied to that connection's pushes and pops. |
| Channel | `CreateChannel` field, not JSON | 0 / null (inherit global) | A value above the global limit is **rejected** with error 15 (`InvalidPayloadLimit`). It is not clamped. |
| Message | `PushOptions` / `PopOptions` | null (inherit) | Can only tighten the cap. The client throws `PayloadTooLargeException` before sending. The server enforces the same minimum. |

`Hello.maxPayloadSize` tells the client the connection ceiling (`min(global, listener)`). See [protocol.md](protocol.md).

Oversize frames are rejected before the body buffer is allocated, logged, and the connection is closed.

## Defaults

| Setting | Default |
| --- | --- |
| `ServerName` | `DomeMQ` |
| `Limits.MaxClients` | 1024 |
| `Limits.MaxChannelsPerClient` | 1024 |
| `Limits.MaxChannels` | 10000 |
| `Limits.MaxPayloadSize` | 16777216 |
| `Limits.MaxMessagesPerChannel` | 100000 |
| `Limits.MaxBytesPerChannel` | null (no per-channel byte cap) |
| `Limits.MaxQueuedBytes` | 1073741824 (1 GiB) |
| `Limits.MaxMessagesPerClient` | 1000000 |
| `Limits.FullBehavior` | `Reject` |
| `Limits.WarningPercent` | 80 |
| `Tcp.NoDelay` | true |
| `Tcp.KeepAlive` | true |
| `Tcp.KeepAliveTimeSeconds` | 60 |
| `Tcp.KeepAliveIntervalSeconds` | 10 |
| `Tcp.ReceiveBufferSize` / `SendBufferSize` | 262144 (256 KiB) |
| `Tcp.Backlog` | 512 |
| `Tcp.LingerSeconds` | null (operating system default) |
| `Keys` | empty |
| `Tls.Enabled` | false |
| `Tls.KeyId` | null (the only key, when exactly one is configured) |
| `Tls.Protocols` | `Tls12`, `Tls13` |
| `Tls.RequireClientCertificate` | false |
| `Tls.AllowUntrustedCertificates` | false |
| `Tls.ClientAuthorities` | empty |
| `Timeouts.HandshakeTimeout` | 15 seconds |
| `Timeouts.IdleTimeout` | 5 minutes (zero disables) |
| `Timeouts.ReadTimeout` / `WriteTimeout` | 2 minutes (zero waits indefinitely) |
| `Timeouts.HeartbeatInterval` | 30 seconds (zero disables server pings) |
| `Authentication.Mode` | `None` |
| `Unix.DeleteStaleSocket` | true |
| `Unix.Permissions` | null (platform default) |
| `Channels.DefaultType` | `Queue` |
| `Channels.DefaultMessageTtl` | null (messages do not expire) |
| `Channels.OrphanChannelTtl` | 5 minutes (zero deletes owned channels on disconnect) |
| `Channels.CleanupInterval` | 30 seconds |
| `Health.Enabled` | false |
| `Health.Host` | `127.0.0.1` |
| `Health.Port` | 5072 |
| `Health.Path` | `/health` |
| `IpAccess.Enabled` | false |
| `IpAccess.Mode` | `Allowlist` |
| `IpAccess.AllowLoopback` | true |
| `Logging.MinimumLevel` | `Information` |
| `Logging.Console` | true |
| `Logging.File` | false |
| `Logging.FilePath` | `domemq.log` |
| TCP listener in the shipped file | enabled, `0.0.0.0:5071` |
| Unix listener in the shipped file | present, disabled, path `/tmp/domemq.sock` |

An embedded `new DomeMqOptions()` starts with an empty listener list. `DomeMqOptions.CreateDefault()` adds the TCP listener on port 5071. The standalone host uses the file as written and does not invent a listener when the list is empty.

## Listeners

`Listeners` is a list. Each entry:

| Property | Type | Meaning |
| --- | --- | --- |
| `Name` | string | Used in logs and as the key for a per-listener IP override. Generated as `listener-N` when empty. |
| `Transport` | string | `Tcp`, `UnixSocket` (`Unix` and `Uds` are aliases). `Udp`, `Kcp`, and `Shm` / `SharedMemory` are reserved and fail startup if enabled. |
| `Enabled` | bool | Default true. Disabled entries are kept in the file but not bound. |
| `Host` | string | TCP bind address. Must be an IP (`0.0.0.0`, `127.0.0.1`, `::`). Default `0.0.0.0`. |
| `Port` | number | TCP port. `0` asks the OS for a port (useful in tests). Default 5071. |
| `Path` | string | Unix socket filesystem path. Required when that listener is enabled. |
| `MaxPayloadSize` | number or null | Listener ceiling. Null inherits the global limit. |
| `NoDelay`, `KeepAlive`, `KeepAliveTimeSeconds`, `KeepAliveIntervalSeconds`, `ReceiveBufferSize`, `SendBufferSize`, `Backlog`, `LingerSeconds` | null or value | Null inherits `Tcp`. |
| `DeleteStaleSocket`, `Permissions` | null or value | Null inherits `Unix`. |
| `Tls` | object or null | Null inherits the top-level `Tls` object, including `KeyId`. A value replaces the whole section. TLS on a Unix listener is a startup error. |
| `IpAccess` | object or null | Null inherits the top-level `IpAccess` object. A value **replaces** it for this listener. Lists are not merged. |

## Limits

| Property | Meaning |
| --- | --- |
| `MaxClients` | Open connections. Extra TCP connections are closed before handshake and logged. |
| `MaxChannelsPerClient` | Channels one connection may create. |
| `MaxChannels` | Channels in the process. |
| `MaxPayloadSize` | Global application-payload ceiling. See precedence above. |
| `MaxMessagesPerChannel` | Global maximum queued messages in one channel. `MaxQueueLengthPerChannel` is an older name for the same value. A create request may set a lower cap. A request above this value is `InvalidQueueLimit`. The effective cap is the minimum of this value, a `Channels.Overrides` entry, and the request. |
| `MaxBytesPerChannel` | Optional byte cap for one channel. Null means no per-channel byte cap. A create request above it is `InvalidByteLimit`. |
| `MaxQueuedBytes` | Total queued application bytes on the server. Default 1 GiB. |
| `MaxMessagesPerClient` | Messages one client has pushed that are still queued. Default 1000000. |
| `FullBehavior` | `Reject` (default), `DiscardNewest`, `DiscardOldest` (alias `DropOldest`), or `ClearAll`. See the protocol spec for queue, stack, and bag eviction. |
| `WarningPercent` | Log and raise `QueuePressure` once each time a channel, the memory cap, or a client crosses this fill percentage. Default 80. The event carries dropped count, cleared count, and fill level for a later GUI. |

## TLS keys

TCP only. Certificate and private key bytes are not written in the JSON file. Each source is exactly one of:

```json
{ "Env": "VAR_NAME" }
{ "File": "/path/to/material.pem" }
```

`Env` is the variable name. The variable's value is PEM text or base64 PKCS#12. A value with literal `\n` sequences and no real newlines is treated as PEM with those sequences turned into newlines. `File` is a PEM or PKCS#12 (`.pfx`) file. Setting both, setting neither, or pasting PEM into the JSON is a startup error.

`Keys` is the list of server identities:

| Property | Required | Meaning |
| --- | --- | --- |
| `Id` | yes | Unique name, compared case-sensitively. |
| `PrivateKey` | yes | PEM private key, or a PKCS#12 blob that also contains the certificate. |
| `Certificate` | yes | PEM certificate (the first `CERTIFICATE` block is the leaf; later blocks are sent as intermediates), or the same PKCS#12 blob as `PrivateKey`. |
| `Password` | no | Env or File whose contents are the password for an encrypted PEM key or a PFX. |

A TLS-enabled listener selects a pair with `KeyId`. When `KeyId` is omitted and `Keys` contains exactly one entry, that entry is the default. Zero keys, or more than one key without `KeyId`, fails startup. An unknown `KeyId`, a duplicate `Id`, a missing environment variable, or a missing file fails startup before the broker listens.

`Tls` itself:

| Property | Default | Meaning |
| --- | --- | --- |
| `Enabled` | false | |
| `KeyId` | null | See the default rule above. |
| `RequireClientCertificate` | false | Mutual TLS. The client must present a certificate. |
| `AllowUntrustedCertificates` | false | Accept any client certificate. Development only. |
| `ClientAuthorities` | empty | Trusted CA certificates or pinned client certificates, each an Env or File source. Required for mTLS unless `AllowUntrustedCertificates` is true. |
| `Protocols` | `Tls12`, `Tls13` | No earlier protocols. |

A listener `Tls` object replaces the top-level section. It does not merge. Repeat `ClientAuthorities` on that listener if it requires client certificates. Unix listeners cannot enable TLS. Certificate revocation lists are not consulted.

### Rotation

Keep two pairs and point the listener at the current one. Replacing the files in place, or changing `KeyId`, is picked up on the next reload (about 5 seconds). Only new handshakes use the new material.

```json
{
  "Keys": [
    {
      "Id": "2026-a",
      "PrivateKey": { "File": "/etc/domemq/2026-a.key" },
      "Certificate": { "File": "/etc/domemq/2026-a.crt" }
    },
    {
      "Id": "2026-b",
      "PrivateKey": { "Env": "DOMEMQ_KEY_2026B" },
      "Certificate": { "Env": "DOMEMQ_CERT_2026B" },
      "Password": { "Env": "DOMEMQ_KEY_2026B_PASSWORD" }
    }
  ],
  "Listeners": [
    {
      "Name": "public",
      "Transport": "Tcp",
      "Port": 5071,
      "Tls": {
        "Enabled": true,
        "KeyId": "2026-a",
        "RequireClientCertificate": true,
        "ClientAuthorities": [
          { "File": "/etc/domemq/client-ca.pem" }
        ],
        "Protocols": [ "Tls12", "Tls13" ]
      }
    },
    {
      "Name": "internal",
      "Transport": "Tcp",
      "Port": 5073,
      "Tls": {
        "Enabled": true,
        "KeyId": "2026-b"
      }
    }
  ]
}
```

To rotate the public listener, install the new files and set `KeyId` to `2026-b`, or overwrite `/etc/domemq/2026-a.key` and `2026-a.crt`. Existing connections stay on the old session. A restart is required only to turn TLS on for a listener that was bound in plaintext, or to change the address or port.

## TCP tuning

Applied to accepted TCP sockets and to the listening socket.

| Property | Default | Meaning |
| --- | --- | --- |
| `NoDelay` | true | Disable Nagle. Small IPC writes are not delayed. |
| `KeepAlive` | true | `SO_KEEPALIVE`. |
| `KeepAliveTimeSeconds` | 60 | Idle time before the first keepalive probe. |
| `KeepAliveIntervalSeconds` | 10 | Gap between probes. |
| `ReceiveBufferSize`, `SendBufferSize` | 262144 | Socket buffers. |
| `Backlog` | 512 | `listen` backlog. |
| `LingerSeconds` | null | Null does not set `SO_LINGER`. |

## Timeouts

| Property | Default | Zero |
| --- | --- | --- |
| `HandshakeTimeout` | 15 seconds | Not allowed. Minimum 1 second, maximum 1 hour. |
| `IdleTimeout` | 5 minutes | Disabled. Otherwise maximum 7 days. |
| `ReadTimeout` | 2 minutes | Wait indefinitely for the rest of a frame after the first byte. Maximum 1 hour. |
| `WriteTimeout` | 2 minutes | Wait indefinitely for a write. Maximum 1 hour. |
| `HeartbeatInterval` | 30 seconds | Server does not send pings. When both heartbeat and idle are set, heartbeat must be shorter than idle. |

The client library has its own `HeartbeatInterval` (default 30 seconds), `RequestTimeout` (30 seconds), `PopTimeout` (30 seconds), and `ReadTimeout` / `WriteTimeout` (2 minutes).

## Authentication

| `Mode` | Behavior |
| --- | --- |
| `None` | `Connect.authToken` is ignored. |
| `SharedKey` | The token must equal one of `Tokens`. Comparison hashes each token and compares the digests in constant time. At least one non-empty token is required. A mismatch is `Unauthorized` and the connection closes. |

## Unix sockets

| Property | Default | Meaning |
| --- | --- | --- |
| `DeleteStaleSocket` | true | Delete a leftover socket file before bind, and delete it on shutdown. |
| `Permissions` | null | Octal string such as `"660"`. Applied on Linux after bind. Ignored on Windows. |

## Channels

| Property | Default | Meaning |
| --- | --- | --- |
| `DefaultType` | `Queue` | Used when `CreateChannel` sends type 0. Also `Stack` or `Bag`. |
| `DefaultMessageTtl` | null | Null or zero: messages never expire. Otherwise a time span such as `"00:05:00"`. Expired messages are dropped on push, pop, and during cleanup. Maximum 30 days. |
| `OrphanChannelTtl` | 5 minutes | How long a channel remains after its owner disconnects. |
| `CleanupInterval` | 30 seconds | Scan interval. Must be positive when `OrphanChannelTtl` is positive, or orphaned channels would never be removed. Zero disables the background scan. Message expiry still happens on push and pop. |
| `Overrides` | empty | Named channel limits. `MaxMessages`, `MaxBytes`, and `FullBehavior` are optional. Numeric effective limits are the minimum of the global value, the override, and the create request. |

## Logging

`DomeMQ.Logging` is what the standalone host reads. The usual ASP.NET `Logging` section is not consulted.

| Property | Default |
| --- | --- |
| `MinimumLevel` | `Information` (`Trace`, `Debug`, `Information`, `Warning`, `Error`, `Critical`, `None`) |
| `Console` | true |
| `File` | false |
| `FilePath` | `domemq.log` |

Rejected IP addresses and oversized frames are logged at warning.

## Health

| Property | Default | Meaning |
| --- | --- | --- |
| `Enabled` | false | |
| `Host` | `127.0.0.1` | Bind address. Use `0.0.0.0` only when the port should be reachable from outside the machine. |
| `Port` | 5072 | |
| `Path` | `/health` | `GET` of this path returns `200` and `{"status":"ok"}`. Anything else returns `404`. |

This is a tiny HTTP response written on a raw TCP socket. It is not the message protocol.

## IP access

Unix domain sockets and shared memory are **not** subject to these rules. TCP is. UDP will be, when it exists.

| Property | Default | Meaning |
| --- | --- | --- |
| `Enabled` | false | While false, every address is accepted. Entries are still parsed so typos fail startup. |
| `Mode` | `Allowlist` | `Allowlist` or `Whitelist` (same thing), or `Blocklist`. |
| `AllowLoopback` | true | When filtering is enabled, `127.0.0.0/8` and `::1` are allowed even if the list would exclude them. Ignored while `Enabled` is false. |
| `Entries` | empty in code; the sample lists `127.0.0.1` and `::1/128` | IPv4 or IPv6 addresses, with an optional CIDR prefix. |

Examples: `192.168.1.1` (a single host, `/32` or `/128`), `192.168.1.0/24`, `::1/128`, `2001:db8::/32`, `0.0.0.0/0`.

IPv4-mapped IPv6 (`::ffff:192.168.1.9`) is matched as IPv4.

A listener `IpAccess` object replaces the global policy for that listener. Omitting it, or setting it to null, inherits the global policy.

Connections that are not allowed are closed immediately after `accept`, before `Connect`, and a warning is logged. No protocol frame is sent.

The broker rebuilds these lists about every 5 seconds. Edit the mounted file, or rely on `reloadOnChange`, and the new list is used for subsequent accepts. Open connections are left alone.

## Docker

Mount a file over one of the paths above, and use environment variables for secrets and small overrides.

```bash
docker run --rm -p 5071:5071 \
  -v "$PWD/appsettings.json:/config/domemq.json:ro" \
  -e DomeMQ__Limits__MaxClients=2048 \
  ghcr.io/hanneshold/domemq:0.1.0
```

The image also contains the sample `appsettings.json` at `/app/appsettings.json`. The mounted file overrides it. Environment variables override both.
