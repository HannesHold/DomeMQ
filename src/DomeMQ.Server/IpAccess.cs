using System.Net;
using System.Net.Sockets;
using DomeMQ;

namespace DomeMQ.Server;

/// <summary>
/// IP allow/block matching for TCP (and, later, UDP). Unix sockets are not evaluated here;
/// <see cref="TransportRegistry.AppliesIpAccess"/> is false for them.
/// </summary>
public sealed class IpAccessControl
{
    private readonly bool _enabled;
    private readonly bool _allowList;
    private readonly bool _allowLoopback;
    private readonly Entry[] _entries;

    private IpAccessControl(bool enabled, bool allowList, bool allowLoopback, Entry[] entries)
    {
        _enabled = enabled;
        _allowList = allowList;
        _allowLoopback = allowLoopback;
        _entries = entries;
    }

    public static IpAccessControl AllowAll { get; } = new(enabled: false, allowList: true, allowLoopback: true, []);

    public bool Enabled => _enabled;

    public static IpAccessControl Create(IpAccessOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var mode = options.Mode.Trim();
        var allowList = mode.Equals("Allowlist", StringComparison.OrdinalIgnoreCase)
            || mode.Equals("Whitelist", StringComparison.OrdinalIgnoreCase);
        if (!allowList && !mode.Equals("Blocklist", StringComparison.OrdinalIgnoreCase))
        {
            throw new FormatException($"Unknown IP access mode '{options.Mode}'. Use Allowlist or Blocklist.");
        }

        var entries = new Entry[options.Entries.Count];
        for (var i = 0; i < options.Entries.Count; i++)
        {
            entries[i] = Parse(options.Entries[i]);
        }

        return new IpAccessControl(options.Enabled, allowList, options.AllowLoopback, entries);
    }

    public bool IsAllowed(IPAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        if (!_enabled)
        {
            return true;
        }

        address = Normalize(address);
        if (_allowLoopback && IPAddress.IsLoopback(address))
        {
            return true;
        }

        var match = false;
        foreach (var entry in _entries)
        {
            if (Contains(address, entry))
            {
                match = true;
                break;
            }
        }

        return _allowList ? match : !match;
    }

    public static IPAddress Normalize(IPAddress address) =>
        address.IsIPv4MappedToIPv6 ? address.MapToIPv4() : address;

    public static Entry Parse(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            throw new FormatException("IP access entry is empty.");
        }

        var trimmed = text.Trim();
        var slash = trimmed.IndexOf('/');
        var addressText = slash < 0 ? trimmed : trimmed[..slash];
        if (!IPAddress.TryParse(addressText, out var address))
        {
            throw new FormatException($"Invalid IP address '{text}'.");
        }

        address = Normalize(address);
        var maxPrefix = address.AddressFamily == AddressFamily.InterNetwork ? 32 : 128;
        var prefix = maxPrefix;
        if (slash >= 0)
        {
            if (!int.TryParse(trimmed[(slash + 1)..], out prefix))
            {
                throw new FormatException($"Invalid CIDR prefix in '{text}'.");
            }
        }

        if (prefix < 0 || prefix > maxPrefix)
        {
            throw new FormatException($"CIDR prefix in '{text}' must be between 0 and {maxPrefix}.");
        }

        return new Entry(address.GetAddressBytes(), prefix);
    }

    public static bool Contains(IPAddress address, IPAddress network, int prefixLength) =>
        Contains(Normalize(address), new Entry(Normalize(network).GetAddressBytes(), prefixLength));

    private static bool Contains(IPAddress address, Entry entry)
    {
        var bytes = address.GetAddressBytes();
        if (bytes.Length != entry.Network.Length)
        {
            return false;
        }

        var fullBytes = entry.Prefix / 8;
        var remainingBits = entry.Prefix % 8;
        for (var i = 0; i < fullBytes; i++)
        {
            if (bytes[i] != entry.Network[i])
            {
                return false;
            }
        }

        if (remainingBits == 0)
        {
            return true;
        }

        var mask = (byte)(0xFF << (8 - remainingBits));
        return (bytes[fullBytes] & mask) == (entry.Network[fullBytes] & mask);
    }

    public readonly record struct Entry(byte[] Network, int Prefix);
}

/// <summary>Global policy plus optional per-listener replacements. Swapped as one snapshot on reload.</summary>
internal sealed class IpAccessDirectory
{
    private Snapshot _snapshot = Snapshot.AllowAll;

    public void Replace(DomeMqOptions options)
    {
        var next = Snapshot.Build(options);
        Volatile.Write(ref _snapshot, next);
    }

    public bool IsAllowed(string listenerName, IPAddress address)
    {
        var snapshot = Volatile.Read(ref _snapshot);
        var policy = snapshot.PerListener.TryGetValue(listenerName, out var specific) ? specific : snapshot.Global;
        return policy.IsAllowed(address);
    }

    private sealed class Snapshot
    {
        public static readonly Snapshot AllowAll = new(IpAccessControl.AllowAll, new Dictionary<string, IpAccessControl>(StringComparer.Ordinal));

        private Snapshot(IpAccessControl global, Dictionary<string, IpAccessControl> perListener)
        {
            Global = global;
            PerListener = perListener;
        }

        public IpAccessControl Global { get; }

        public Dictionary<string, IpAccessControl> PerListener { get; }

        public static Snapshot Build(DomeMqOptions options)
        {
            var global = IpAccessControl.Create(options.IpAccess);
            var map = new Dictionary<string, IpAccessControl>(StringComparer.Ordinal);
            foreach (var listener in options.Listeners)
            {
                if (listener.IpAccess is null)
                {
                    continue;
                }

                map[listener.Name] = IpAccessControl.Create(listener.IpAccess);
            }

            return new Snapshot(global, map);
        }
    }
}
