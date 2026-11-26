using DomeMQ;

namespace DomeMQ.Server;

/// <summary>Documented defaults for <see cref="DomeMqOptions"/> and the shipped <c>appsettings.json</c>.</summary>
public static class DomeMqDefaults
{
    public const string ServerName = "DomeMQ";
    public const int MaxClients = 1024;
    public const int MaxChannelsPerClient = 1024;
    public const int MaxChannels = 10_000;
    public const int MaxPayloadSize = 16 * 1024 * 1024;
    public const int MaxMessagesPerChannel = 100_000;
    public const int MaxQueueLengthPerChannel = MaxMessagesPerChannel;
    public const long MaxQueuedBytes = 1024L * 1024L * 1024L;
    public const int MaxMessagesPerClient = 1_000_000;
    public const int WarningPercent = 80;
    public const string FullBehavior = "Reject";
    public const int SocketBufferSize = 256 * 1024;
    public const int Backlog = 512;
    public const bool NoDelay = true;
    public const bool KeepAlive = true;
    public const int KeepAliveTimeSeconds = 60;
    public const int KeepAliveIntervalSeconds = 10;
    public const int Port = 5071;
    public const int HealthPort = 5072;
    public const string HealthHost = "127.0.0.1";
    public const string HealthPath = "/health";
    public const string DefaultChannelType = "Queue";
    public const bool AllowLoopback = true;
    public const bool DeleteStaleSocket = true;
    public const string LogLevel = "Information";
    public const string LogFilePath = "domemq.log";

    public static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(15);
    public static readonly TimeSpan IdleTimeout = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan ReadTimeout = TimeSpan.FromMinutes(2);
    public static readonly TimeSpan WriteTimeout = TimeSpan.FromMinutes(2);
    public static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan OrphanChannelTtl = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan CleanupInterval = TimeSpan.FromSeconds(30);
}

/// <summary>
/// Broker configuration. Bind this type from the <c>DomeMQ</c> section of appsettings.json or domemq.json.
/// Nullable listener fields inherit the matching top-level section.
/// </summary>
public sealed class DomeMqOptions
{
    public string ServerName { get; set; } = DomeMqDefaults.ServerName;

    public List<ListenerOptions> Listeners { get; set; } = [];

    public LimitOptions Limits { get; set; } = new();

    public TcpOptions Tcp { get; set; } = new();

    /// <summary>Named TLS key pairs. Material is loaded from an environment variable or a file, never inline.</summary>
    public List<KeyPairOptions> Keys { get; set; } = [];

    public TlsOptions Tls { get; set; } = new();

    public TimeoutOptions Timeouts { get; set; } = new();

    public AuthenticationOptions Authentication { get; set; } = new();

    public UnixOptions Unix { get; set; } = new();

    public ChannelOptions Channels { get; set; } = new();

    public HealthOptions Health { get; set; } = new();

    public IpAccessOptions IpAccess { get; set; } = new();

    public LoggingOptions Logging { get; set; } = new();

    /// <summary>
    /// Options for an embedded broker that listens on TCP port 5071.
    /// A configuration file does not call this; it binds onto a new instance instead.
    /// </summary>
    public static DomeMqOptions CreateDefault()
    {
        var options = new DomeMqOptions();
        options.Listeners.Add(new ListenerOptions
        {
            Name = "tcp",
            Transport = "Tcp",
            Enabled = true,
            Host = "0.0.0.0",
            Port = DomeMqDefaults.Port
        });
        return options;
    }
}

public sealed class ListenerOptions
{
    public string Name { get; set; } = "";

    /// <summary><c>Tcp</c>, <c>UnixSocket</c>, or a reserved name (<c>Udp</c>, <c>Kcp</c>).</summary>
    public string Transport { get; set; } = "Tcp";

    public bool Enabled { get; set; } = true;

    public string Host { get; set; } = "0.0.0.0";

    public int Port { get; set; } = DomeMqDefaults.Port;

    public string? Path { get; set; }

    public bool? DeleteStaleSocket { get; set; }

    /// <summary>Octal Unix permission bits such as <c>660</c>. Null inherits <see cref="UnixOptions.Permissions"/>.</summary>
    public string? Permissions { get; set; }

    /// <summary>Null inherits the server global <see cref="LimitOptions.MaxPayloadSize"/>.</summary>
    public int? MaxPayloadSize { get; set; }

    public bool? NoDelay { get; set; }

    public bool? KeepAlive { get; set; }

    public int? KeepAliveTimeSeconds { get; set; }

    public int? KeepAliveIntervalSeconds { get; set; }

    public int? ReceiveBufferSize { get; set; }

    public int? SendBufferSize { get; set; }

    public int? Backlog { get; set; }

    /// <summary>Null leaves the operating system linger default.</summary>
    public int? LingerSeconds { get; set; }

    /// <summary>Null inherits the top-level <see cref="DomeMqOptions.Tls"/> section.</summary>
    public TlsOptions? Tls { get; set; }

    /// <summary>Null inherits the top-level <see cref="DomeMqOptions.IpAccess"/> section. A value replaces it for this listener.</summary>
    public IpAccessOptions? IpAccess { get; set; }
}

public sealed class LimitOptions
{
    public int MaxClients { get; set; } = DomeMqDefaults.MaxClients;

    public int MaxChannelsPerClient { get; set; } = DomeMqDefaults.MaxChannelsPerClient;

    public int MaxChannels { get; set; } = DomeMqDefaults.MaxChannels;

    /// <summary>Hard upper bound on one application payload, in bytes. Default is 16 MiB.</summary>
    public int MaxPayloadSize { get; set; } = DomeMqDefaults.MaxPayloadSize;

    /// <summary>Global maximum queued messages in one channel. Default is 100000.</summary>
    public int MaxMessagesPerChannel { get; set; } = DomeMqDefaults.MaxMessagesPerChannel;

    /// <summary>Earlier name for <see cref="MaxMessagesPerChannel"/>. Setting either property updates the same limit.</summary>
    public int MaxQueueLengthPerChannel
    {
        get => MaxMessagesPerChannel;
        set => MaxMessagesPerChannel = value;
    }

    /// <summary>Optional per-channel byte cap. Null means no per-channel byte cap.</summary>
    public long? MaxBytesPerChannel { get; set; }

    /// <summary>Total queued application bytes across every channel. Default is 1 GiB.</summary>
    public long MaxQueuedBytes { get; set; } = DomeMqDefaults.MaxQueuedBytes;

    /// <summary>Queued messages one client has pushed and that nobody has consumed yet. Default is 1000000.</summary>
    public int MaxMessagesPerClient { get; set; } = DomeMqDefaults.MaxMessagesPerClient;

    /// <summary><c>Reject</c>, <c>DiscardNewest</c>, <c>DiscardOldest</c> (<c>DropOldest</c>), or <c>ClearAll</c>.</summary>
    public string FullBehavior { get; set; } = DomeMqDefaults.FullBehavior;

    /// <summary>Log and raise a pressure event once each time fill crosses this percent. Default is 80.</summary>
    public int WarningPercent { get; set; } = DomeMqDefaults.WarningPercent;
}

public sealed class TcpOptions
{
    public bool NoDelay { get; set; } = DomeMqDefaults.NoDelay;

    public bool KeepAlive { get; set; } = DomeMqDefaults.KeepAlive;

    public int KeepAliveTimeSeconds { get; set; } = DomeMqDefaults.KeepAliveTimeSeconds;

    public int KeepAliveIntervalSeconds { get; set; } = DomeMqDefaults.KeepAliveIntervalSeconds;

    public int ReceiveBufferSize { get; set; } = DomeMqDefaults.SocketBufferSize;

    public int SendBufferSize { get; set; } = DomeMqDefaults.SocketBufferSize;

    public int Backlog { get; set; } = DomeMqDefaults.Backlog;

    public int? LingerSeconds { get; set; }
}

/// <summary>
/// A named server key pair. <see cref="PrivateKey"/> and <see cref="Certificate"/> are required.
/// <see cref="Password"/> is optional and is used for an encrypted PEM key or a PFX.
/// </summary>
public sealed class KeyPairOptions
{
    public string Id { get; set; } = "";

    public MaterialSource? PrivateKey { get; set; }

    public MaterialSource? Certificate { get; set; }

    public MaterialSource? Password { get; set; }
}

/// <summary>
/// Where to read secret or key material. Set exactly one of <see cref="Env"/> or <see cref="File"/>.
/// Inline PEM or passwords are rejected.
/// </summary>
public sealed class MaterialSource
{
    /// <summary>Name of an environment variable. The value is PEM text or base64 PKCS#12.</summary>
    public string? Env { get; set; }

    /// <summary>Filesystem path of a PEM or PKCS#12 file.</summary>
    public string? File { get; set; }
}

public sealed class TlsOptions
{
    public bool Enabled { get; set; }

    /// <summary>
    /// <see cref="KeyPairOptions.Id"/> to present. When null and exactly one key is configured, that key is used.
    /// </summary>
    public string? KeyId { get; set; }

    public bool RequireClientCertificate { get; set; }

    /// <summary>Accept any client certificate. Leave false outside local development.</summary>
    public bool AllowUntrustedCertificates { get; set; }

    /// <summary>Trusted client CA certificates or pinned public certificates. Each entry is Env or File.</summary>
    public List<MaterialSource> ClientAuthorities { get; set; } = [];

    public List<string> Protocols { get; set; } = ["Tls12", "Tls13"];
}

public sealed class TimeoutOptions
{
    public TimeSpan HandshakeTimeout { get; set; } = DomeMqDefaults.HandshakeTimeout;

    /// <summary>Zero disables the idle timeout.</summary>
    public TimeSpan IdleTimeout { get; set; } = DomeMqDefaults.IdleTimeout;

    /// <summary>Zero waits indefinitely for the remainder of a frame.</summary>
    public TimeSpan ReadTimeout { get; set; } = DomeMqDefaults.ReadTimeout;

    /// <summary>Zero waits indefinitely for a write.</summary>
    public TimeSpan WriteTimeout { get; set; } = DomeMqDefaults.WriteTimeout;

    /// <summary>How often the server pings a quiet connection. Zero disables server pings.</summary>
    public TimeSpan HeartbeatInterval { get; set; } = DomeMqDefaults.HeartbeatInterval;
}

public sealed class AuthenticationOptions
{
    /// <summary><c>None</c> or <c>SharedKey</c>.</summary>
    public string Mode { get; set; } = "None";

    public List<string> Tokens { get; set; } = [];
}

public sealed class UnixOptions
{
    public bool DeleteStaleSocket { get; set; } = DomeMqDefaults.DeleteStaleSocket;

    /// <summary>Octal mode string such as <c>660</c>. Null keeps the platform default.</summary>
    public string? Permissions { get; set; }
}

public sealed class ChannelOptions
{
    /// <summary><c>Queue</c>, <c>Stack</c>, or <c>Bag</c>. Used when a client sends channel type 0.</summary>
    public string DefaultType { get; set; } = DomeMqDefaults.DefaultChannelType;

    /// <summary>Null or zero means messages do not expire.</summary>
    public TimeSpan? DefaultMessageTtl { get; set; }

    /// <summary>How long a channel survives after its owner disconnects. Zero deletes it immediately.</summary>
    public TimeSpan OrphanChannelTtl { get; set; } = DomeMqDefaults.OrphanChannelTtl;

    /// <summary>How often orphaned channels and expired messages are scanned. Zero disables the background scan.</summary>
    public TimeSpan CleanupInterval { get; set; } = DomeMqDefaults.CleanupInterval;

    /// <summary>Optional limits for specific channel names. Applied together with the create request: the effective numeric limit is the minimum of the set levels.</summary>
    public List<ChannelLimitOverride> Overrides { get; set; } = [];
}

public sealed class ChannelLimitOverride
{
    public string Name { get; set; } = "";

    /// <summary>Null inherits <see cref="LimitOptions.MaxMessagesPerChannel"/>.</summary>
    public int? MaxMessages { get; set; }

    /// <summary>Null inherits <see cref="LimitOptions.MaxBytesPerChannel"/>.</summary>
    public long? MaxBytes { get; set; }

    /// <summary>Null inherits <see cref="LimitOptions.FullBehavior"/>.</summary>
    public string? FullBehavior { get; set; }
}

public sealed class HealthOptions
{
    public bool Enabled { get; set; }

    public string Host { get; set; } = DomeMqDefaults.HealthHost;

    public int Port { get; set; } = DomeMqDefaults.HealthPort;

    public string Path { get; set; } = DomeMqDefaults.HealthPath;
}

public sealed class IpAccessOptions
{
    public bool Enabled { get; set; }

    /// <summary><c>Allowlist</c> (alias <c>Whitelist</c>) or <c>Blocklist</c>.</summary>
    public string Mode { get; set; } = "Allowlist";

    /// <summary>
    /// When true, 127.0.0.0/8 and ::1 are allowed even if the list would exclude them.
    /// Has no effect while <see cref="Enabled"/> is false.
    /// </summary>
    public bool AllowLoopback { get; set; } = DomeMqDefaults.AllowLoopback;

    public List<string> Entries { get; set; } = [];
}

public sealed class LoggingOptions
{
    public string MinimumLevel { get; set; } = DomeMqDefaults.LogLevel;

    public bool Console { get; set; } = true;

    public bool File { get; set; }

    public string FilePath { get; set; } = DomeMqDefaults.LogFilePath;
}

public enum BrokerLogLevel
{
    Trace = 0,
    Debug = 1,
    Information = 2,
    Warning = 3,
    Error = 4,
    Critical = 5,
    None = 6
}

/// <summary>Logging sink implemented by the host. The server library does not reference Microsoft.Extensions.Logging.</summary>
public interface IBrokerLogger
{
    void Log(BrokerLogLevel level, string category, string message, Exception? exception = null);
}

public sealed class DomeMqConfigurationException : DomeMqException
{
    public IReadOnlyList<string> Errors { get; }

    public DomeMqConfigurationException(IReadOnlyList<string> errors)
        : base(errors.Count == 0 ? "Configuration is invalid." : string.Join(Environment.NewLine, errors))
    {
        Errors = errors;
    }
}
