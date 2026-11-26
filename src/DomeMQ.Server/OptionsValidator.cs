using System.Net;
using System.Security.Authentication;
using DomeMQ;

namespace DomeMQ.Server;

public static class DomeMqOptionsValidator
{
    public static void Normalize(DomeMqOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Listeners ??= [];
        options.Limits ??= new LimitOptions();
        options.Tcp ??= new TcpOptions();
        options.Tls ??= new TlsOptions();
        options.Timeouts ??= new TimeoutOptions();
        options.Authentication ??= new AuthenticationOptions();
        options.Unix ??= new UnixOptions();
        options.Channels ??= new ChannelOptions();
        options.Health ??= new HealthOptions();
        options.IpAccess ??= new IpAccessOptions();
        options.Logging ??= new LoggingOptions();
        options.Tls.Protocols ??= [];
        options.Authentication.Tokens ??= [];
        options.IpAccess.Entries ??= [];

        for (var i = 0; i < options.Listeners.Count; i++)
        {
            var listener = options.Listeners[i] ??= new ListenerOptions();
            if (string.IsNullOrWhiteSpace(listener.Name))
            {
                listener.Name = $"listener-{i}";
            }

            listener.IpAccess?.Entries ??= [];
            if (listener.Tls is not null)
            {
                listener.Tls.Protocols ??= [];
            }
        }
    }

    public static IReadOnlyList<string> Validate(DomeMqOptions options)
    {
        Normalize(options);
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(options.ServerName) || options.ServerName.Length > 128)
        {
            errors.Add("ServerName must be 1 to 128 characters.");
        }

        ValidateLimits(options.Limits, errors);
        ValidateTimeouts(options.Timeouts, errors);
        ValidateAuthentication(options.Authentication, errors);
        ValidateChannels(options.Channels, errors);
        ValidateLogging(options.Logging, errors);
        ValidateIp(options.IpAccess, "IpAccess", errors);
        ValidateUnixMode(options.Unix.Permissions, "Unix.Permissions", errors);

        if (!TryParseChannelKind(options.Channels.DefaultType, out _))
        {
            errors.Add("Channels.DefaultType must be Queue, Stack, or Bag.");
        }

        if (options.Channels.OrphanChannelTtl > TimeSpan.Zero && options.Channels.CleanupInterval <= TimeSpan.Zero)
        {
            errors.Add("Channels.CleanupInterval must be positive when Channels.OrphanChannelTtl is positive.");
        }

        var names = new HashSet<string>(StringComparer.Ordinal);
        var binds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < options.Listeners.Count; i++)
        {
            ValidateListener(options, options.Listeners[i], i, names, binds, errors);
        }

        if (options.Health.Enabled)
        {
            if (!IPAddress.TryParse(options.Health.Host, out _))
            {
                errors.Add("Health.Host must be an IP address.");
            }

            if (options.Health.Port is < 0 or > 65535)
            {
                errors.Add("Health.Port must be between 0 and 65535.");
            }

            if (string.IsNullOrWhiteSpace(options.Health.Path) || !options.Health.Path.StartsWith('/'))
            {
                errors.Add("Health.Path must start with '/'.");
            }
        }

        return errors;
    }

    public static void ValidateAndThrow(DomeMqOptions options)
    {
        var errors = Validate(options);
        if (errors.Count > 0)
        {
            throw new DomeMqConfigurationException(errors);
        }
    }

    public static bool TryParseFullBehavior(string text, out bool dropOldest)
    {
        dropOldest = false;
        if (text.Equals("Reject", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (text.Equals("DropOldest", StringComparison.OrdinalIgnoreCase))
        {
            dropOldest = true;
            return true;
        }

        return false;
    }

    public static bool TryParseChannelKind(string text, out ChannelKind kind)
    {
        if (text.Equals("Queue", StringComparison.OrdinalIgnoreCase))
        {
            kind = ChannelKind.Queue;
            return true;
        }

        if (text.Equals("Stack", StringComparison.OrdinalIgnoreCase))
        {
            kind = ChannelKind.Stack;
            return true;
        }

        if (text.Equals("Bag", StringComparison.OrdinalIgnoreCase))
        {
            kind = ChannelKind.Bag;
            return true;
        }

        kind = default;
        return false;
    }

    public static bool TryParseLogLevel(string text, out BrokerLogLevel level)
    {
        if (Enum.TryParse(text, ignoreCase: true, out level) && Enum.IsDefined(level))
        {
            return true;
        }

        level = BrokerLogLevel.Information;
        return false;
    }

    public static bool TryParseProtocols(IReadOnlyList<string> names, out SslProtocols protocols, out string error)
    {
        protocols = SslProtocols.None;
        if (names.Count == 0)
        {
            error = "TLS protocols must include Tls12, Tls13, or both.";
            return false;
        }

        foreach (var name in names)
        {
            if (name.Equals("Tls12", StringComparison.OrdinalIgnoreCase))
            {
                protocols |= SslProtocols.Tls12;
            }
            else if (name.Equals("Tls13", StringComparison.OrdinalIgnoreCase))
            {
                protocols |= SslProtocols.Tls13;
            }
            else
            {
                error = $"Unsupported TLS protocol '{name}'. Only Tls12 and Tls13 are allowed.";
                return false;
            }
        }

        error = "";
        return true;
    }

    public static int? ParseUnixMode(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return null;
        }

        return Convert.ToInt32(text, 8);
    }

    private static void ValidateLimits(LimitOptions limits, List<string> errors)
    {
        if (limits.MaxClients < 1)
        {
            errors.Add("Limits.MaxClients must be at least 1.");
        }

        if (limits.MaxChannels < 1)
        {
            errors.Add("Limits.MaxChannels must be at least 1.");
        }

        if (limits.MaxChannelsPerClient < 1)
        {
            errors.Add("Limits.MaxChannelsPerClient must be at least 1.");
        }

        if (limits.MaxChannelsPerClient > limits.MaxChannels)
        {
            errors.Add("Limits.MaxChannelsPerClient cannot exceed Limits.MaxChannels.");
        }

        if (limits.MaxQueueLengthPerChannel < 1)
        {
            errors.Add("Limits.MaxQueueLengthPerChannel must be at least 1.");
        }

        if (limits.MaxPayloadSize < 1 || limits.MaxPayloadSize > ProtocolConstants.MaxConfigurablePayload)
        {
            errors.Add($"Limits.MaxPayloadSize must be between 1 and {ProtocolConstants.MaxConfigurablePayload}.");
        }

        if (!TryParseFullBehavior(limits.FullBehavior, out _))
        {
            errors.Add("Limits.FullBehavior must be Reject or DropOldest.");
        }
    }

    private static void ValidateTimeouts(TimeoutOptions timeouts, List<string> errors)
    {
        if (timeouts.HandshakeTimeout < TimeSpan.FromSeconds(1) || timeouts.HandshakeTimeout > TimeSpan.FromHours(1))
        {
            errors.Add("Timeouts.HandshakeTimeout must be between 1 second and 1 hour.");
        }

        ValidateOptionalTimeout(timeouts.IdleTimeout, TimeSpan.FromDays(7), "Timeouts.IdleTimeout", errors);
        ValidateOptionalTimeout(timeouts.ReadTimeout, TimeSpan.FromHours(1), "Timeouts.ReadTimeout", errors);
        ValidateOptionalTimeout(timeouts.WriteTimeout, TimeSpan.FromHours(1), "Timeouts.WriteTimeout", errors);
        ValidateOptionalTimeout(timeouts.HeartbeatInterval, TimeSpan.FromHours(1), "Timeouts.HeartbeatInterval", errors);
        if (timeouts.HeartbeatInterval > TimeSpan.Zero
            && timeouts.IdleTimeout > TimeSpan.Zero
            && timeouts.HeartbeatInterval >= timeouts.IdleTimeout)
        {
            errors.Add("Timeouts.HeartbeatInterval must be shorter than Timeouts.IdleTimeout.");
        }
    }

    private static void ValidateOptionalTimeout(TimeSpan value, TimeSpan max, string name, List<string> errors)
    {
        if (value < TimeSpan.Zero || value > max)
        {
            errors.Add($"{name} must be zero (disabled) or at most {max}.");
        }
    }

    private static void ValidateAuthentication(AuthenticationOptions auth, List<string> errors)
    {
        var mode = auth.Mode.Trim();
        if (mode.Equals("None", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (!mode.Equals("SharedKey", StringComparison.OrdinalIgnoreCase))
        {
            errors.Add("Authentication.Mode must be None or SharedKey.");
            return;
        }

        if (auth.Tokens.Count == 0 || auth.Tokens.Any(string.IsNullOrEmpty))
        {
            errors.Add("Authentication.Tokens must contain at least one non-empty token when Mode is SharedKey.");
        }
    }

    private static void ValidateChannels(ChannelOptions channels, List<string> errors)
    {
        if (channels.DefaultMessageTtl is TimeSpan ttl && (ttl < TimeSpan.Zero || ttl > TimeSpan.FromDays(30)))
        {
            errors.Add("Channels.DefaultMessageTtl must be null, zero, or at most 30 days.");
        }

        if (channels.OrphanChannelTtl < TimeSpan.Zero || channels.OrphanChannelTtl > TimeSpan.FromDays(30))
        {
            errors.Add("Channels.OrphanChannelTtl must be between zero and 30 days.");
        }

        if (channels.CleanupInterval < TimeSpan.Zero || channels.CleanupInterval > TimeSpan.FromDays(1))
        {
            errors.Add("Channels.CleanupInterval must be between zero and 1 day.");
        }
    }

    private static void ValidateLogging(LoggingOptions logging, List<string> errors)
    {
        if (!TryParseLogLevel(logging.MinimumLevel, out _))
        {
            errors.Add("Logging.MinimumLevel must be Trace, Debug, Information, Warning, Error, Critical, or None.");
        }

        if (logging.File && string.IsNullOrWhiteSpace(logging.FilePath))
        {
            errors.Add("Logging.FilePath is required when Logging.File is true.");
        }
    }

    private static void ValidateIp(IpAccessOptions ip, string path, List<string> errors)
    {
        try
        {
            _ = IpAccessControl.Create(ip);
        }
        catch (FormatException ex)
        {
            errors.Add($"{path}: {ex.Message}");
        }
    }

    private static void ValidateListener(
        DomeMqOptions root,
        ListenerOptions listener,
        int index,
        HashSet<string> names,
        HashSet<string> binds,
        List<string> errors)
    {
        if (!names.Add(listener.Name))
        {
            errors.Add($"Duplicate listener name '{listener.Name}'.");
        }

        if (!TransportRegistry.IsKnown(listener.Transport))
        {
            errors.Add($"Listener '{listener.Name}' uses unknown transport '{listener.Transport}'.");
            return;
        }

        if (listener.IpAccess is not null)
        {
            ValidateIp(listener.IpAccess, $"Listeners[{index}].IpAccess", errors);
        }

        if (listener.MaxPayloadSize is int payload
            && (payload < 1 || payload > root.Limits.MaxPayloadSize))
        {
            errors.Add($"Listener '{listener.Name}' MaxPayloadSize must be between 1 and Limits.MaxPayloadSize.");
        }

        ValidateUnixMode(listener.Permissions, $"Listener '{listener.Name}' Permissions", errors);

        if (!listener.Enabled)
        {
            return;
        }

        if (!TransportRegistry.IsImplemented(listener.Transport))
        {
            errors.Add($"Listener '{listener.Name}' uses transport '{listener.Transport}', which is not implemented yet.");
            return;
        }

        var transport = TransportRegistry.Normalize(listener.Transport);
        if (transport == "tcp")
        {
            if (!IPAddress.TryParse(listener.Host, out _))
            {
                errors.Add($"Listener '{listener.Name}' Host must be an IP address.");
            }

            if (listener.Port is < 0 or > 65535)
            {
                errors.Add($"Listener '{listener.Name}' Port must be between 0 and 65535.");
            }

            var tls = listener.Tls ?? root.Tls;
            if (tls.Enabled)
            {
                ValidateTls(tls, $"Listener '{listener.Name}'", errors);
            }

            if (!binds.Add($"tcp:{listener.Host}:{listener.Port}"))
            {
                errors.Add($"Listener '{listener.Name}' duplicates TCP bind {listener.Host}:{listener.Port}.");
            }
        }
        else if (transport == "unix")
        {
            if (string.IsNullOrWhiteSpace(listener.Path))
            {
                errors.Add($"Listener '{listener.Name}' requires Path for a Unix socket.");
            }
            else if (!binds.Add($"unix:{listener.Path}"))
            {
                errors.Add($"Listener '{listener.Name}' duplicates Unix socket path '{listener.Path}'.");
            }

            var tls = listener.Tls ?? root.Tls;
            if (tls.Enabled)
            {
                errors.Add($"Listener '{listener.Name}' cannot enable TLS on a Unix socket. TLS is supported for TCP only.");
            }
        }
    }

    private static void ValidateTls(TlsOptions tls, string path, List<string> errors)
    {
        var hasPem = !string.IsNullOrWhiteSpace(tls.CertificatePath) || !string.IsNullOrWhiteSpace(tls.KeyPath);
        var hasPfx = !string.IsNullOrWhiteSpace(tls.PfxPath);
        if (hasPem && hasPfx)
        {
            errors.Add($"{path} TLS must set either CertificatePath/KeyPath or PfxPath, not both.");
        }
        else if (hasPfx)
        {
            if (!File.Exists(tls.PfxPath))
            {
                errors.Add($"{path} TLS PfxPath '{tls.PfxPath}' was not found.");
            }
        }
        else if (hasPem)
        {
            if (string.IsNullOrWhiteSpace(tls.CertificatePath) || string.IsNullOrWhiteSpace(tls.KeyPath))
            {
                errors.Add($"{path} TLS requires both CertificatePath and KeyPath.");
            }
            else
            {
                if (!File.Exists(tls.CertificatePath))
                {
                    errors.Add($"{path} TLS CertificatePath '{tls.CertificatePath}' was not found.");
                }

                if (!File.Exists(tls.KeyPath))
                {
                    errors.Add($"{path} TLS KeyPath '{tls.KeyPath}' was not found.");
                }
            }
        }
        else
        {
            errors.Add($"{path} TLS is enabled but no certificate is configured.");
        }

        if (!TryParseProtocols(tls.Protocols, out _, out var protocolError))
        {
            errors.Add($"{path} {protocolError}");
        }
    }

    private static void ValidateUnixMode(string? text, string path, List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return;
        }

        try
        {
            var mode = Convert.ToInt32(text, 8);
            if (mode is < 0 or > 0x1FF)
            {
                errors.Add($"{path} must be an octal mode between 0 and 777.");
            }
        }
        catch (FormatException)
        {
            errors.Add($"{path} must be an octal mode such as 660.");
        }
    }
}
