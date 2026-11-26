using System.Diagnostics;
using DomeMQ;
using DomeMQ.Client;

namespace DomeMQ.Inspector.Services;

public sealed class ScenarioOutcome
{
    public required bool Passed { get; init; }

    public required string Detail { get; init; }

    public required TimeSpan Duration { get; init; }

    public required long LogFrom { get; init; }

    public required long LogTo { get; init; }
}

public sealed class ScenarioDefinition
{
    public required string Name { get; init; }

    public required string Explanation { get; init; }

    public required Func<ScenarioContext, CancellationToken, Task<ScenarioOutcome>> Run { get; init; }
}

public sealed class ScenarioContext
{
    public required DomeMqClient? Client { get; init; }

    public required DomeMqClientOptions Options { get; init; }

    public required TrafficLog Log { get; init; }

    public required bool TlsEnabled { get; init; }

    public required bool ClientCertificateConfigured { get; init; }
}

public static class ScenarioCatalog
{
    public static IReadOnlyList<ScenarioDefinition> All { get; } =
    [
        new()
        {
            Name = "Handshake",
            Explanation = "Sends Ping and waits for Pong. Checks that the session completed Connect and Hello.",
            Run = async (context, token) => await Measure(context, async client =>
            {
                await client.PingAsync(token).ConfigureAwait(false);
                return "Ping returned Pong.";
            })
        },
        new()
        {
            Name = "Queue roundtrip",
            Explanation = "Creates a Queue, pushes two payloads, and pops them in FIFO order.",
            Run = (context, token) => Roundtrip(context, token, ChannelKind.Queue, "one", "two", "one", "two")
        },
        new()
        {
            Name = "Stack roundtrip",
            Explanation = "Creates a Stack, pushes two payloads, and pops them in LIFO order.",
            Run = (context, token) => Roundtrip(context, token, ChannelKind.Stack, "one", "two", "two", "one")
        },
        new()
        {
            Name = "Bag roundtrip",
            Explanation = "Creates a Bag and checks that both payloads come back. Order is not asserted.",
            Run = async (context, token) => await Measure(context, async client =>
            {
                var name = NameOf("bag");
                await client.CreateChannelAsync(name, ChannelKind.Bag, cancellationToken: token).ConfigureAwait(false);
                await client.PushAsync(name, "red"u8.ToArray(), cancellationToken: token).ConfigureAwait(false);
                await client.PushAsync(name, "blue"u8.ToArray(), cancellationToken: token).ConfigureAwait(false);
                var first = await client.PopAsync(name, new PopOptions { Timeout = TimeSpan.FromSeconds(2) }, token).ConfigureAwait(false);
                var second = await client.PopAsync(name, new PopOptions { Timeout = TimeSpan.FromSeconds(2) }, token).ConfigureAwait(false);
                var got = new HashSet<string>(StringComparer.Ordinal)
                {
                    Text(first),
                    Text(second)
                };
                if (!got.SetEquals(["red", "blue"]))
                {
                    throw new InvalidOperationException($"Bag returned [{string.Join(", ", got)}].");
                }

                return "Both bag payloads were received.";
            })
        },
        new()
        {
            Name = "Payload limit exceeded",
            Explanation = "Creates a channel whose payload limit is 32 bytes and pushes 64 bytes. The server must reject it.",
            Run = async (context, token) => await Measure(context, async client =>
            {
                var name = NameOf("limit");
                await client.CreateChannelAsync(name, ChannelKind.Queue, new CreateChannelOptions { MaxPayloadSize = 32 }, token).ConfigureAwait(false);
                var options = Copy(context.Options);
                options.ClientName = "inspector-limit";
                options.HeartbeatInterval = TimeSpan.Zero;
                await using var sender = await DomeMqClient.ConnectAsync(options, token).ConfigureAwait(false);
                try
                {
                    await sender.PushAsync(name, new byte[64], cancellationToken: token).ConfigureAwait(false);
                }
                catch (PayloadTooLargeException)
                {
                    return "The server rejected a 64-byte push on a 32-byte channel.";
                }

                throw new InvalidOperationException("The oversized push was accepted.");
            })
        },
        new()
        {
            Name = "Full policy: Reject",
            Explanation = "A one-message queue rejects the second push with ChannelFull.",
            Run = (context, token) => Policy(context, token, FullPolicy.Reject, expectDiscarded: false, expectError: true, expectPop: "a")
        },
        new()
        {
            Name = "Full policy: DiscardNewest",
            Explanation = "A full queue keeps the message already stored and discards the incoming one.",
            Run = (context, token) => Policy(context, token, FullPolicy.DiscardNewest, expectDiscarded: true, expectError: false, expectPop: "a")
        },
        new()
        {
            Name = "Full policy: DiscardOldest",
            Explanation = "A full queue drops the oldest message and stores the incoming one.",
            Run = (context, token) => Policy(context, token, FullPolicy.DiscardOldest, expectDiscarded: false, expectError: false, expectPop: "b")
        },
        new()
        {
            Name = "Full policy: ClearAll",
            Explanation = "A full queue is emptied, then only the incoming message is stored.",
            Run = (context, token) => Policy(context, token, FullPolicy.ClearAll, expectDiscarded: false, expectError: false, expectPop: "b")
        },
        new()
        {
            Name = "Whitelist reject",
            Explanation = "Opens a second connection. Passes only when the server refuses it. With the default AllowLoopback setting this fails on purpose.",
            Run = Whitelist
        },
        new()
        {
            Name = "TLS",
            Explanation = "Connects again with the TLS options from the Connect screen and sends Ping. Fails with a setup note when TLS is not enabled.",
            Run = (context, token) => Secure(context, token, mutual: false)
        },
        new()
        {
            Name = "mTLS",
            Explanation = "Connects with the configured client certificate. Fails with a setup note when no client certificate is set.",
            Run = (context, token) => Secure(context, token, mutual: true)
        },
        new()
        {
            Name = "Throughput",
            Explanation = "Pushes 200 small messages and reports the accepted rate. Passes when every push is accepted.",
            Run = async (context, token) => await Measure(context, async client =>
            {
                var name = NameOf("rate");
                await client.CreateChannelAsync(name, ChannelKind.Queue, cancellationToken: token).ConfigureAwait(false);
                var payload = new byte[64];
                var started = Stopwatch.StartNew();
                for (var i = 0; i < 200; i++)
                {
                    var result = await client.PushAsync(name, payload, cancellationToken: token).ConfigureAwait(false);
                    if (result.Discarded)
                    {
                        throw new InvalidOperationException($"Message {i} was discarded.");
                    }
                }

                started.Stop();
                var rate = 200 / Math.Max(0.001, started.Elapsed.TotalSeconds);
                return $"200 accepted in {started.Elapsed.TotalMilliseconds:0} ms ({rate:0} msgs/s).";
            })
        }
    ];

    private static async Task<ScenarioOutcome> Roundtrip(
        ScenarioContext context,
        CancellationToken token,
        ChannelKind kind,
        string first,
        string second,
        string expectFirst,
        string expectSecond)
    {
        return await Measure(context, async client =>
        {
            var name = NameOf(kind.ToString().ToLowerInvariant());
            await client.CreateChannelAsync(name, kind, cancellationToken: token).ConfigureAwait(false);
            await client.PushAsync(name, System.Text.Encoding.UTF8.GetBytes(first), cancellationToken: token).ConfigureAwait(false);
            await client.PushAsync(name, System.Text.Encoding.UTF8.GetBytes(second), cancellationToken: token).ConfigureAwait(false);
            var poppedFirst = Text(await client.PopAsync(name, new PopOptions { Timeout = TimeSpan.FromSeconds(2) }, token).ConfigureAwait(false));
            var poppedSecond = Text(await client.PopAsync(name, new PopOptions { Timeout = TimeSpan.FromSeconds(2) }, token).ConfigureAwait(false));
            if (poppedFirst != expectFirst || poppedSecond != expectSecond)
            {
                throw new InvalidOperationException($"Got {poppedFirst}, {poppedSecond}; expected {expectFirst}, {expectSecond}.");
            }

            return $"{kind} returned {poppedFirst}, {poppedSecond}.";
        }).ConfigureAwait(false);
    }

    private static async Task<ScenarioOutcome> Policy(
        ScenarioContext context,
        CancellationToken token,
        FullPolicy policy,
        bool expectDiscarded,
        bool expectError,
        string expectPop)
    {
        return await Measure(context, async client =>
        {
            var name = NameOf(policy.ToString().ToLowerInvariant());
            await client.CreateChannelAsync(name, ChannelKind.Queue, new CreateChannelOptions
            {
                MaxQueueLength = 1,
                FullPolicy = policy
            }, token).ConfigureAwait(false);
            await client.PushAsync(name, "a"u8.ToArray(), cancellationToken: token).ConfigureAwait(false);
            string second;
            try
            {
                var result = await client.PushAsync(name, "b"u8.ToArray(), cancellationToken: token).ConfigureAwait(false);
                if (expectError)
                {
                    throw new InvalidOperationException("The second push was accepted.");
                }

                if (result.Discarded != expectDiscarded)
                {
                    throw new InvalidOperationException(result.Discarded ? "The second push was discarded." : "The second push was stored.");
                }

                second = result.Discarded ? "discarded" : "stored";
            }
            catch (DomeMqException ex) when (ex.Code == ErrorCode.ChannelFull)
            {
                if (!expectError)
                {
                    throw;
                }

                second = "rejected";
            }

            if (expectError)
            {
                return $"Second push was {second}.";
            }

            var popped = Text(await client.PopAsync(name, new PopOptions { Timeout = TimeSpan.FromSeconds(2) }, token).ConfigureAwait(false));
            if (popped != expectPop)
            {
                throw new InvalidOperationException($"Pop returned '{popped}', expected '{expectPop}'.");
            }

            return $"Second push was {second}. Pop returned '{popped}'.";
        }).ConfigureAwait(false);
    }

    private static async Task<ScenarioOutcome> Whitelist(ScenarioContext context, CancellationToken token)
    {
        var started = Stopwatch.StartNew();
        var from = context.Log.Mark();
        try
        {
            var options = Copy(context.Options);
            options.ClientName = "inspector-whitelist";
            options.HeartbeatInterval = TimeSpan.Zero;
            await using var extra = await DomeMqClient.ConnectAsync(options, token).ConfigureAwait(false);
            started.Stop();
            return new ScenarioOutcome
            {
                Passed = false,
                Detail = "The server accepted the connection. To see a reject, set IpAccess.Enabled, Mode Allowlist, AllowLoopback false, and leave this address out of the list.",
                Duration = started.Elapsed,
                LogFrom = from,
                LogTo = context.Log.Mark()
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            started.Stop();
            return new ScenarioOutcome
            {
                Passed = true,
                Detail = "Connection was refused: " + ex.Message,
                Duration = started.Elapsed,
                LogFrom = from,
                LogTo = context.Log.Mark()
            };
        }
    }

    private static async Task<ScenarioOutcome> Secure(ScenarioContext context, CancellationToken token, bool mutual)
    {
        var ready = mutual ? context.ClientCertificateConfigured : context.TlsEnabled;
        if (!ready)
        {
            return new ScenarioOutcome
            {
                Passed = false,
                Detail = mutual
                    ? "Set a client certificate (PFX or PEM) on the Connect screen and enable mTLS on the server, then run this again."
                    : "Enable TLS on the Connect screen and on the server listener, then run this again.",
                Duration = TimeSpan.Zero,
                LogFrom = context.Log.Mark(),
                LogTo = context.Log.Mark()
            };
        }

        var started = Stopwatch.StartNew();
        var from = context.Log.Mark();
        try
        {
            var options = Copy(context.Options);
            options.ClientName = mutual ? "inspector-mtls" : "inspector-tls";
            options.HeartbeatInterval = TimeSpan.Zero;
            if (!mutual && options.Tls is not null)
            {
                options.Tls.CertificatePath = null;
                options.Tls.KeyPath = null;
                options.Tls.PfxPath = null;
            }

            await using var extra = await DomeMqClient.ConnectAsync(options, token).ConfigureAwait(false);
            await extra.PingAsync(token).ConfigureAwait(false);
            started.Stop();
            return new ScenarioOutcome
            {
                Passed = true,
                Detail = mutual ? "mTLS handshake and Ping succeeded." : "TLS handshake and Ping succeeded.",
                Duration = started.Elapsed,
                LogFrom = from,
                LogTo = context.Log.Mark()
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            started.Stop();
            return Fail(context, from, started.Elapsed, ex.Message);
        }
    }

    private static async Task<ScenarioOutcome> Measure(ScenarioContext context, Func<DomeMqClient, Task<string>> body)
    {
        if (context.Client is null)
        {
            return new ScenarioOutcome
            {
                Passed = false,
                Detail = "Connect on the Connect screen first. These scenarios use that session.",
                Duration = TimeSpan.Zero,
                LogFrom = context.Log.Mark(),
                LogTo = context.Log.Mark()
            };
        }

        var started = Stopwatch.StartNew();
        var from = context.Log.Mark();
        try
        {
            var detail = await body(context.Client).ConfigureAwait(false);
            started.Stop();
            return new ScenarioOutcome
            {
                Passed = true,
                Detail = detail,
                Duration = started.Elapsed,
                LogFrom = from,
                LogTo = context.Log.Mark()
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            started.Stop();
            return Fail(context, from, started.Elapsed, ex.Message);
        }
    }

    private static ScenarioOutcome Fail(ScenarioContext context, long from, TimeSpan duration, string detail) => new()
    {
        Passed = false,
        Detail = detail,
        Duration = duration,
        LogFrom = from,
        LogTo = context.Log.Mark()
    };

    private static DomeMqClientOptions Copy(DomeMqClientOptions source) => new()
    {
        Transport = source.Transport,
        Host = source.Host,
        Port = source.Port,
        Path = source.Path,
        ClientName = source.ClientName,
        AuthToken = source.AuthToken,
        MaxPayloadSize = source.MaxPayloadSize,
        RequestTimeout = source.RequestTimeout,
        PopTimeout = TimeSpan.FromSeconds(5),
        HeartbeatInterval = TimeSpan.Zero,
        ReadTimeout = source.ReadTimeout,
        WriteTimeout = source.WriteTimeout,
        TrafficObserver = source.TrafficObserver,
        Tls = source.Tls is null ? null : new TlsClientOptions
        {
            Enabled = source.Tls.Enabled,
            TargetHost = source.Tls.TargetHost,
            AllowUntrustedCertificates = source.Tls.AllowUntrustedCertificates,
            CertificatePath = source.Tls.CertificatePath,
            KeyPath = source.Tls.KeyPath,
            KeyPassword = source.Tls.KeyPassword,
            PfxPath = source.Tls.PfxPath,
            PfxPassword = source.Tls.PfxPassword,
            Protocols = source.Tls.Protocols
        }
    };

    private static string NameOf(string prefix) => prefix + "-" + Guid.NewGuid().ToString("N")[..8];

    private static string Text(IReadOnlyList<DomeMqMessage> messages) =>
        messages.Count == 0 ? "" : System.Text.Encoding.UTF8.GetString(messages[0].Body);
}
