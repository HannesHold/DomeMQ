using DomeMQ.Server;
using Microsoft.Extensions.Configuration;

namespace DomeMQ.Inspector.Services;

public static class ConfigLoader
{
    public static DomeMqOptions Load(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return DomeMqOptions.CreateDefault();
        }

        var configuration = new ConfigurationBuilder()
            .AddJsonFile(Path.GetFullPath(path), optional: false, reloadOnChange: false)
            .Build();
        var options = new DomeMqOptions();
        var section = configuration.GetSection("DomeMQ");
        IConfiguration source = section.Exists() ? section : configuration;
        source.Bind(options);
        var errors = DomeMqOptionsValidator.Validate(options);
        if (errors.Count > 0)
        {
            throw new InvalidOperationException(string.Join(Environment.NewLine, errors));
        }

        return options;
    }

    public static string Summarize(DomeMqOptions options)
    {
        var lines = new List<string>
        {
            $"Server {options.ServerName}",
            $"Limits  clients {options.Limits.MaxClients}, channels {options.Limits.MaxChannels}, payload {options.Limits.MaxPayloadSize}, messages/channel {options.Limits.MaxMessagesPerChannel}, queued bytes {options.Limits.MaxQueuedBytes}, messages/client {options.Limits.MaxMessagesPerClient}, full {options.Limits.FullBehavior}, warning {options.Limits.WarningPercent}%"
        };
        if (options.Limits.MaxBytesPerChannel is long bytes)
        {
            lines.Add($"Per-channel byte cap {bytes}");
        }

        foreach (var listener in options.Listeners.Where(static item => item.Enabled))
        {
            var where = string.Equals(listener.Transport, "UnixSocket", StringComparison.OrdinalIgnoreCase)
                ? listener.Path
                : $"{listener.Host}:{listener.Port}";
            var tls = listener.Tls ?? options.Tls;
            var tlsText = !tls.Enabled
                ? "plaintext"
                : tls.KeyMode == "PerClient"
                    ? $"TLS per client{(string.IsNullOrWhiteSpace(tls.FallbackKeyId) ? "" : $", fallback {tls.FallbackKeyId}")}"
                    : $"TLS key {tls.KeyId ?? "(single key)"}";
            lines.Add($"Listener {listener.Name} {listener.Transport} {where} {tlsText}");
        }

        if (options.IpAccess.Enabled)
        {
            lines.Add($"IP {options.IpAccess.Mode} loopback {(options.IpAccess.AllowLoopback ? "allowed" : "denied")}: {string.Join(", ", options.IpAccess.Entries)}");
        }
        else
        {
            lines.Add("IP rules off");
        }

        return string.Join(Environment.NewLine, lines);
    }
}
