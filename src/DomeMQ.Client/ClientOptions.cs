using System.Security.Authentication;
using DomeMQ;

namespace DomeMQ.Client;

/// <summary>How to reach a broker and the client-wide payload ceiling.</summary>
public sealed class DomeMqClientOptions
{
    public string Transport { get; set; } = "tcp";

    public string Host { get; set; } = "127.0.0.1";

    public int Port { get; set; } = 5071;

    public string? Path { get; set; }

    public string ClientName { get; set; } = "domemq-client";

    public string? AuthToken { get; set; }

    /// <summary>
    /// Client-wide application payload limit. Null inherits the limit advertised by the server.
    /// </summary>
    public int? MaxPayloadSize { get; set; }

    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How long a pop waits when <see cref="PopOptions.Timeout"/> is null.</summary>
    public TimeSpan PopTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How often the client sends <c>Ping</c>. Zero disables client heartbeats.</summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Deadline for the bytes of one frame after the first byte arrives. Zero waits indefinitely.</summary>
    public TimeSpan ReadTimeout { get; set; } = TimeSpan.FromMinutes(2);

    public TimeSpan WriteTimeout { get; set; } = TimeSpan.FromMinutes(2);

    public TlsClientOptions? Tls { get; set; }
}

public sealed class TlsClientOptions
{
    public bool Enabled { get; set; }

    public string? TargetHost { get; set; }

    public bool AllowUntrustedCertificates { get; set; }

    public string? CertificatePath { get; set; }

    public string? KeyPath { get; set; }

    public string? KeyPassword { get; set; }

    public string? PfxPath { get; set; }

    public string? PfxPassword { get; set; }

    public SslProtocols Protocols { get; set; } = SslProtocols.Tls12 | SslProtocols.Tls13;
}

/// <summary>Per-send options. Null limits inherit the client and server limits.</summary>
public sealed class PushOptions
{
    public int? MaxPayloadSize { get; set; }

    /// <summary>Opaque label stored with the payload and returned on pop. DomeMQ does not interpret it.</summary>
    public string? ContentType { get; set; }

    /// <summary>
    /// When true, the client returns after the frame is written and does not wait for <c>Ack</c>.
    /// The server still sends <c>Error</c> on failure, but this client does not observe it for that call.
    /// </summary>
    public bool NoAck { get; set; }
}

/// <summary>Per-receive options. Null limits inherit the client and server limits.</summary>
public sealed class PopOptions
{
    public int? MaxPayloadSize { get; set; }

    public int MaxMessages { get; set; } = 1;

    /// <summary>Null uses <see cref="DomeMqClientOptions.PopTimeout"/>. Zero returns immediately.</summary>
    public TimeSpan? Timeout { get; set; }
}

public sealed class CreateChannelOptions
{
    /// <summary>Null or zero uses the server default queue length, capped by the server maximum.</summary>
    public int? MaxQueueLength { get; set; }

    /// <summary>Null inherits the server global limit. A value above the server global limit is rejected.</summary>
    public int? MaxPayloadSize { get; set; }
}

public sealed class ChannelInfo
{
    public required string Name { get; init; }

    public required ChannelKind Kind { get; init; }

    public required int MaxQueueLength { get; init; }

    public required int MaxPayloadSize { get; init; }
}

/// <summary>One popped application message. <see cref="Body"/> is opaque.</summary>
public sealed class DomeMqMessage
{
    public DomeMqMessage(byte[] body, string contentType)
    {
        Body = body;
        ContentType = contentType;
    }

    public byte[] Body { get; }

    public string ContentType { get; }
}
