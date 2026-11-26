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
        options.Channels.Overrides ??= [];
        options.Health ??= new HealthOptions();
        options.IpAccess ??= new IpAccessOptions();
        options.Logging ??= new LoggingOptions();
        options.Keys ??= [];
        options.Tls.Protocols ??= [];
        options.Tls.ClientAuthorities ??= [];
        if (!string.IsNullOrWhiteSpace(options.Tls.KeyId))
        {
            options.Tls.KeyId = options.Tls.KeyId.Trim();
        }
        options.Authentication.Tokens ??= [];
        options.IpAccess.Entries ??= [];
        for (var i = 0; i < options.Keys.Count; i++)
        {
            options.Keys[i] ??= new KeyPairOptions();
            options.Keys[i].Id = options.Keys[i].Id?.Trim() ?? "";
        }

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
                listener.Tls.ClientAuthorities ??= [];
                if (!string.IsNullOrWhiteSpace(listener.Tls.KeyId))
                {
                    listener.Tls.KeyId = listener.Tls.KeyId.Trim();
                }
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
        ValidateChannels(options.Channels, options.Limits, errors);
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

        ValidateKeys(options, errors);

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

        if (errors.Count == 0)
        {
            try
            {
                var bundle = MaterialBundle.Read(options);
                using var loaded = KeySnapshot.Create(options, bundle);
            }
            catch (DomeMqConfigurationException ex)
            {
                errors.Add(ex.Message);
            }
        }

        return errors;
    }

    public static TlsOptions EffectiveTls(DomeMqOptions root, ListenerOptions listener) => listener.Tls ?? root.Tls;

    /// <summary>
    /// Explicit <see cref="TlsOptions.KeyId"/>, or the only configured key when <paramref name="tls"/> omits it.
    /// </summary>
    public static string? ResolveKeyId(DomeMqOptions root, TlsOptions tls)
    {
        if (!string.IsNullOrWhiteSpace(tls.KeyId))
        {
            return tls.KeyId.Trim();
        }

        return root.Keys.Count == 1 ? root.Keys[0].Id : null;
    }

    public static void ValidateAndThrow(DomeMqOptions options)
    {
        var errors = Validate(options);
        if (errors.Count > 0)
        {
            throw new DomeMqConfigurationException(errors);
        }
    }

    public static bool TryParseFullPolicy(string text, out FullPolicy policy)
    {
        if (text.Equals("Reject", StringComparison.OrdinalIgnoreCase))
        {
            policy = FullPolicy.Reject;
            return true;
        }

        if (text.Equals("DiscardNewest", StringComparison.OrdinalIgnoreCase))
        {
            policy = FullPolicy.DiscardNewest;
            return true;
        }

        if (text.Equals("DiscardOldest", StringComparison.OrdinalIgnoreCase)
            || text.Equals("DropOldest", StringComparison.OrdinalIgnoreCase))
        {
            policy = FullPolicy.DiscardOldest;
            return true;
        }

        if (text.Equals("ClearAll", StringComparison.OrdinalIgnoreCase))
        {
            policy = FullPolicy.ClearAll;
            return true;
        }

        policy = FullPolicy.Reject;
        return false;
    }

    public static bool TryParseFullBehavior(string text, out bool dropOldest)
    {
        dropOldest = false;
        if (!TryParseFullPolicy(text, out var policy))
        {
            return false;
        }

        dropOldest = policy == FullPolicy.DiscardOldest;
        return true;
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

        if (limits.MaxMessagesPerChannel < 1)
        {
            errors.Add("Limits.MaxMessagesPerChannel must be at least 1.");
        }

        if (limits.MaxBytesPerChannel is long perChannel && perChannel < 1)
        {
            errors.Add("Limits.MaxBytesPerChannel must be at least 1 when it is set.");
        }

        if (limits.MaxQueuedBytes < 1)
        {
            errors.Add("Limits.MaxQueuedBytes must be at least 1.");
        }

        if (limits.MaxBytesPerChannel is long channelBytes && channelBytes > limits.MaxQueuedBytes)
        {
            errors.Add("Limits.MaxBytesPerChannel cannot exceed Limits.MaxQueuedBytes.");
        }

        if (limits.MaxMessagesPerClient < 1)
        {
            errors.Add("Limits.MaxMessagesPerClient must be at least 1.");
        }

        if (limits.WarningPercent is < 1 or > 100)
        {
            errors.Add("Limits.WarningPercent must be between 1 and 100.");
        }

        if (limits.MaxPayloadSize < 1 || limits.MaxPayloadSize > ProtocolConstants.MaxConfigurablePayload)
        {
            errors.Add($"Limits.MaxPayloadSize must be between 1 and {ProtocolConstants.MaxConfigurablePayload}.");
        }

        if (!TryParseFullPolicy(limits.FullBehavior, out _))
        {
            errors.Add("Limits.FullBehavior must be Reject, DiscardNewest, DiscardOldest, or ClearAll.");
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

    private static void ValidateChannels(ChannelOptions channels, LimitOptions limits, List<string> errors)
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

        var names = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < channels.Overrides.Count; i++)
        {
            var item = channels.Overrides[i];
            if (item is null || string.IsNullOrWhiteSpace(item.Name))
            {
                errors.Add($"Channels.Overrides[{i}].Name is required.");
                continue;
            }

            if (!names.Add(item.Name))
            {
                errors.Add($"Channels.Overrides contains a duplicate name '{item.Name}'.");
            }

            if (item.MaxMessages is int messages && (messages < 1 || messages > limits.MaxMessagesPerChannel))
            {
                errors.Add($"Channels.Overrides '{item.Name}' MaxMessages must be between 1 and Limits.MaxMessagesPerChannel.");
            }

            if (item.MaxBytes is long bytes)
            {
                if (bytes < 1 || bytes > limits.MaxQueuedBytes)
                {
                    errors.Add($"Channels.Overrides '{item.Name}' MaxBytes must be between 1 and Limits.MaxQueuedBytes.");
                }

                if (limits.MaxBytesPerChannel is long cap && bytes > cap)
                {
                    errors.Add($"Channels.Overrides '{item.Name}' MaxBytes cannot exceed Limits.MaxBytesPerChannel.");
                }
            }

            if (!string.IsNullOrWhiteSpace(item.FullBehavior) && !TryParseFullPolicy(item.FullBehavior, out _))
            {
                errors.Add($"Channels.Overrides '{item.Name}' FullBehavior must be Reject, DiscardNewest, DiscardOldest, or ClearAll.");
            }
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

            var tls = EffectiveTls(root, listener);
            if (tls.Enabled)
            {
                ValidateTls(root, tls, $"Listener '{listener.Name}'", errors);
            }

            if (listener.Port != 0 && !binds.Add($"tcp:{listener.Host}:{listener.Port}"))
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

    private static void ValidateKeys(DomeMqOptions options, List<string> errors)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < options.Keys.Count; i++)
        {
            var key = options.Keys[i];
            var path = $"Keys[{i}]";
            if (string.IsNullOrWhiteSpace(key.Id))
            {
                errors.Add($"{path}.Id is required.");
            }
            else if (!seen.Add(key.Id))
            {
                errors.Add($"Duplicate key id '{key.Id}'.");
            }

            ValidateMaterial(key.PrivateKey, $"{path} '{key.Id}' PrivateKey", required: true, errors);
            ValidateMaterial(key.Certificate, $"{path} '{key.Id}' Certificate", required: true, errors);
            ValidateMaterial(key.Password, $"{path} '{key.Id}' Password", required: false, errors);
        }

        ValidateMaterialList(options.Tls.ClientAuthorities, "Tls.ClientAuthorities", errors);
        foreach (var listener in options.Listeners)
        {
            if (listener.Tls is null)
            {
                continue;
            }

            ValidateMaterialList(listener.Tls.ClientAuthorities, $"Listener '{listener.Name}' ClientAuthorities", errors);
        }
    }

    private static void ValidateTls(DomeMqOptions root, TlsOptions tls, string path, List<string> errors)
    {
        if (!TryParseProtocols(tls.Protocols, out _, out var protocolError))
        {
            errors.Add($"{path} {protocolError}");
        }

        var keyId = ResolveKeyId(root, tls);
        if (keyId is null)
        {
            errors.Add(root.Keys.Count == 0
                ? $"{path} TLS is enabled but no keys are configured. Add a Keys entry."
                : $"{path} TLS is enabled without KeyId, and {root.Keys.Count} keys are configured. Set KeyId. KeyId may be omitted only when exactly one key is defined.");
        }
        else if (!root.Keys.Any(key => key.Id == keyId))
        {
            errors.Add($"{path} TLS KeyId '{keyId}' does not match a Keys entry.");
        }

        if (tls.RequireClientCertificate && !tls.AllowUntrustedCertificates && tls.ClientAuthorities.Count == 0 && root.Tls.ClientAuthorities.Count == 0)
        {
            errors.Add($"{path} TLS requires a client certificate but no ClientAuthorities are configured. Add trusted CA or public certificates via Env or File, or set AllowUntrustedCertificates for local development.");
        }

        if (tls.RequireClientCertificate && !tls.AllowUntrustedCertificates && tls.ClientAuthorities.Count == 0 && !ReferenceEquals(tls, root.Tls) && root.Tls.ClientAuthorities.Count > 0)
        {
            errors.Add($"{path} TLS replaces the top-level Tls section and requires a client certificate, but this listener has no ClientAuthorities. Repeat them on the listener or set AllowUntrustedCertificates.");
        }
    }

    private static void ValidateMaterialList(List<MaterialSource> sources, string path, List<string> errors)
    {
        for (var i = 0; i < sources.Count; i++)
        {
            ValidateMaterial(sources[i], $"{path}[{i}]", required: true, errors);
        }
    }

    private static void ValidateMaterial(MaterialSource? source, string path, bool required, List<string> errors)
    {
        if (source is null)
        {
            if (required)
            {
                errors.Add($"{path} is required and must be {{\"Env\":\"VAR_NAME\"}} or {{\"File\":\"/path\"}}. Inline key material is not allowed.");
            }

            return;
        }

        var hasEnv = !string.IsNullOrWhiteSpace(source.Env);
        var hasFile = !string.IsNullOrWhiteSpace(source.File);
        if (hasEnv == hasFile)
        {
            errors.Add($"{path} must set exactly one of Env or File. Inline key material is not allowed.");
            return;
        }

        if (hasEnv)
        {
            if (source.Env!.Contains("-----BEGIN", StringComparison.Ordinal) || source.Env.Contains('\n'))
            {
                errors.Add($"{path} Env must be an environment variable name, not key material. Inline key material is not allowed.");
                return;
            }

            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(source.Env)))
            {
                errors.Add($"{path} environment variable '{source.Env}' is missing or empty.");
            }
        }
        else
        {
            if (source.File!.Contains('\n') || source.File.Contains("-----BEGIN", StringComparison.Ordinal))
            {
                errors.Add($"{path} File must be a filesystem path. Inline key material is not allowed.");
                return;
            }

            if (!File.Exists(source.File))
            {
                errors.Add($"{path} file '{source.File}' was not found.");
            }
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
