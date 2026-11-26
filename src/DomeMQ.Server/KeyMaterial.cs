using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using DomeMQ;

namespace DomeMQ.Server;

/// <summary>
/// Loads TLS key pairs and rebuilds them when the config reload sees new material.
/// Certificates already handed to a live connection stay valid until the process stops.
/// </summary>
internal sealed class KeyRing : IDisposable
{
    private readonly object _gate = new();
    private readonly List<KeySnapshot> _retired = [];
    private volatile KeySnapshot _current = KeySnapshot.Empty;
    private string _fingerprint = "";
    private bool _loaded;

    public bool Load(DomeMqOptions options)
    {
        var bundle = MaterialBundle.Read(options);
        lock (_gate)
        {
            if (_loaded && _fingerprint == bundle.Fingerprint)
            {
                return false;
            }

            var next = KeySnapshot.Create(options, bundle);
            if (_loaded)
            {
                _retired.Add(_current);
            }

            _current = next;
            _fingerprint = bundle.Fingerprint;
            _loaded = true;
            return true;
        }
    }

    public TlsHandshake Require(string listenerName)
    {
        var current = _current;
        if (!current.Listeners.TryGetValue(listenerName, out var handshake))
        {
            throw new InvalidOperationException($"TLS material for listener '{listenerName}' is not loaded. A restart is required to enable TLS on a listener that was started without it.");
        }

        return handshake;
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _current.Dispose();
            _current = KeySnapshot.Empty;
            foreach (var retired in _retired)
            {
                retired.Dispose();
            }

            _retired.Clear();
            _loaded = false;
            _fingerprint = "";
        }
    }
}

internal sealed class KeySnapshot : IDisposable
{
    public static KeySnapshot Empty { get; } = new(new Dictionary<string, TlsHandshake>(StringComparer.Ordinal), []);

    private readonly List<X509Certificate2> _owned;
    private bool _disposed;

    private KeySnapshot(Dictionary<string, TlsHandshake> listeners, List<X509Certificate2> owned)
    {
        Listeners = listeners;
        _owned = owned;
    }

    public Dictionary<string, TlsHandshake> Listeners { get; }

    public static KeySnapshot Create(DomeMqOptions options, MaterialBundle bundle)
    {
        var owned = new List<X509Certificate2>();
        var certificates = new Dictionary<string, LoadedCertificate>(StringComparer.Ordinal);
        try
        {
            foreach (var pair in options.Keys)
            {
                var material = bundle.Keys[pair.Id];
                var loaded = KeyParser.ParsePair(pair.Id, material);
                if (!certificates.TryAdd(pair.Id, loaded))
                {
                    owned.Add(loaded.Certificate);
                    foreach (var extra in loaded.Chain)
                    {
                        owned.Add(extra);
                    }

                    throw new DomeMqConfigurationException([$"Duplicate key id '{pair.Id}'."]);
                }
                owned.Add(loaded.Certificate);
                foreach (var extra in loaded.Chain)
                {
                    owned.Add(extra);
                }
            }

            var authorities = new Dictionary<string, X509Certificate2Collection>(StringComparer.Ordinal);
            foreach (var (name, sources) in bundle.Authorities)
            {
                var collection = KeyParser.ParseAuthorities(name, sources);
                authorities.Add(name, collection);
                foreach (var certificate in collection)
                {
                    owned.Add(certificate);
                }
            }

            var listeners = new Dictionary<string, TlsHandshake>(StringComparer.Ordinal);
            foreach (var listener in options.Listeners.Where(static item => item.Enabled))
            {
                if (TransportRegistry.Normalize(listener.Transport) != "tcp")
                {
                    continue;
                }

                var tls = listener.Tls ?? options.Tls;
                if (!tls.Enabled)
                {
                    continue;
                }

                var keyId = DomeMqOptionsValidator.ResolveKeyId(options, tls)
                    ?? throw new DomeMqConfigurationException([$"Listener '{listener.Name}' TLS has no key."]);
                if (!certificates.TryGetValue(keyId, out var certificate))
                {
                    throw new DomeMqConfigurationException([$"Listener '{listener.Name}' TLS KeyId '{keyId}' does not match a Keys entry."]);
                }

                var authorityKey = listener.Tls is null ? "" : listener.Name;
                authorities.TryGetValue(authorityKey, out var roots);
                if (!DomeMqOptionsValidator.TryParseProtocols(tls.Protocols, out var protocols, out var protocolError))
                {
                    throw new DomeMqConfigurationException([protocolError]);
                }

                listeners.Add(listener.Name, new TlsHandshake
                {
                    ServerCertificate = certificate.Certificate,
                    AdditionalCertificates = certificate.Chain.Count == 0 ? null : certificate.Chain,
                    RequireClientCertificate = tls.RequireClientCertificate,
                    AllowUntrustedClientCertificates = tls.AllowUntrustedCertificates,
                    Protocols = protocols,
                    TrustedClientRoots = roots
                });
            }

            return new KeySnapshot(listeners, owned);
        }
        catch
        {
            foreach (var certificate in owned)
            {
                certificate.Dispose();
            }

            throw;
        }
    }

    public void Dispose()
    {
        if (_disposed || ReferenceEquals(this, Empty))
        {
            return;
        }

        _disposed = true;
        foreach (var certificate in _owned)
        {
            certificate.Dispose();
        }
    }
}

internal sealed class LoadedCertificate(X509Certificate2 certificate, X509Certificate2Collection chain)
{
    public X509Certificate2 Certificate { get; } = certificate;

    public X509Certificate2Collection Chain { get; } = chain;
}

internal sealed class MaterialBundle
{
    public required string Fingerprint { get; init; }

    public required Dictionary<string, PairMaterial> Keys { get; init; }

    public required Dictionary<string, List<LoadedMaterial>> Authorities { get; init; }

    public static MaterialBundle Read(DomeMqOptions options)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var keys = new Dictionary<string, PairMaterial>(StringComparer.Ordinal);
        foreach (var pair in options.Keys)
        {
            if (pair.PrivateKey is null || pair.Certificate is null)
            {
                throw new DomeMqConfigurationException([$"Key '{pair.Id}' is missing PrivateKey or Certificate."]);
            }

            Append(hash, pair.Id);
            var privateKey = MaterialReader.Read(pair.PrivateKey, hash);
            var certificate = MaterialReader.Read(pair.Certificate, hash);
            string? password = null;
            if (pair.Password is not null)
            {
                password = MaterialReader.Read(pair.Password, hash).Text.Trim();
            }

            keys.Add(pair.Id, new PairMaterial(privateKey, certificate, password));
        }

        var authorities = new Dictionary<string, List<LoadedMaterial>>(StringComparer.Ordinal);
        ReadAuthorities("", options.Tls.ClientAuthorities, authorities, hash);
        foreach (var listener in options.Listeners)
        {
            if (listener.Tls is null)
            {
                continue;
            }

            Append(hash, "listener-auth:" + listener.Name);
            ReadAuthorities(listener.Name, listener.Tls.ClientAuthorities, authorities, hash);
        }

        foreach (var listener in options.Listeners.Where(static item => item.Enabled))
        {
            var tls = listener.Tls ?? options.Tls;
            Append(hash, listener.Name);
            Append(hash, tls.Enabled ? "1" : "0");
            Append(hash, tls.KeyId ?? "");
            Append(hash, tls.RequireClientCertificate ? "1" : "0");
            Append(hash, tls.AllowUntrustedCertificates ? "1" : "0");
            Append(hash, string.Join(',', tls.Protocols));
        }

        return new MaterialBundle
        {
            Fingerprint = Convert.ToHexString(hash.GetHashAndReset()),
            Keys = keys,
            Authorities = authorities
        };
    }

    private static void ReadAuthorities(
        string name,
        List<MaterialSource> sources,
        Dictionary<string, List<LoadedMaterial>> target,
        IncrementalHash hash)
    {
        if (sources.Count == 0)
        {
            return;
        }

        Append(hash, "auth:" + name);
        var list = new List<LoadedMaterial>(sources.Count);
        foreach (var source in sources)
        {
            list.Add(MaterialReader.Read(source, hash));
        }

        target.Add(name, list);
    }

    private static void Append(IncrementalHash hash, string text) =>
        hash.AppendData(Encoding.UTF8.GetBytes(text));
}

internal readonly record struct PairMaterial(LoadedMaterial PrivateKey, LoadedMaterial Certificate, string? Password);

internal readonly record struct LoadedMaterial(byte[] Bytes, string Text)
{
    public bool IsPem => Text.Contains("-----BEGIN", StringComparison.Ordinal);
}

internal static class MaterialReader
{
    public static LoadedMaterial Read(MaterialSource source, IncrementalHash? hash)
    {
        byte[] bytes;
        if (!string.IsNullOrWhiteSpace(source.File))
        {
            try
            {
                bytes = File.ReadAllBytes(source.File);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                throw new DomeMqConfigurationException([$"Could not read '{source.File}'. {ex.Message}"]);
            }
        }
        else
        {
            var value = Environment.GetEnvironmentVariable(source.Env!);
            if (string.IsNullOrEmpty(value))
            {
                throw new DomeMqConfigurationException([$"Environment variable '{source.Env}' is missing or empty."]);
            }
            if (!value.Contains('\n') && value.Contains("\\n", StringComparison.Ordinal))
            {
                value = value.Replace("\\n", "\n", StringComparison.Ordinal);
            }

            bytes = Encoding.UTF8.GetBytes(value);
        }

        hash?.AppendData(bytes);
        return new LoadedMaterial(bytes, Encoding.UTF8.GetString(bytes));
    }
}

internal static class KeyParser
{
    public static LoadedCertificate ParsePair(string id, PairMaterial material)
    {
        try
        {
            if (material.Certificate.IsPem && material.PrivateKey.IsPem)
            {
                return ParsePem(id, material.Certificate.Text, material.PrivateKey.Text, material.Password);
            }

            var pfx = TryPkcs12(material.PrivateKey) ?? TryPkcs12(material.Certificate);
            if (pfx is null)
            {
                throw new DomeMqConfigurationException([$"Key '{id}' is neither PEM nor PKCS#12. Put PEM text or base64 PKCS#12 in the environment variable, or point File at a PEM or PFX file."]);
            }

            var certificate = Persist(X509CertificateLoader.LoadPkcs12(pfx, material.Password));
            if (!certificate.HasPrivateKey)
            {
                certificate.Dispose();
                throw new DomeMqConfigurationException([$"Key '{id}' does not contain a private key."]);
            }

            return new LoadedCertificate(certificate, new X509Certificate2Collection());
        }
        catch (DomeMqConfigurationException)
        {
            throw;
        }
        catch (Exception ex) when (ex is CryptographicException or InvalidDataException)
        {
            throw new DomeMqConfigurationException([$"Key '{id}' could not be loaded. Check that the PEM or PFX matches and that Password is set for encrypted material. {ex.Message}"]);
        }
    }

    public static X509Certificate2Collection ParseAuthorities(string name, IReadOnlyList<LoadedMaterial> sources)
    {
        var label = string.IsNullOrEmpty(name) ? "Tls.ClientAuthorities" : $"Listener '{name}' ClientAuthorities";
        var collection = new X509Certificate2Collection();
        try
        {
            foreach (var source in sources)
            {
                if (source.IsPem)
                {
                    collection.ImportFromPem(source.Text);
                    continue;
                }

                var pfx = TryPkcs12(source);
                if (pfx is null)
                {
                    throw new DomeMqConfigurationException([$"{label} must be a PEM certificate or a PKCS#12 file."]);
                }

                collection.Add(X509CertificateLoader.LoadPkcs12(pfx, password: null));
            }
        }
        catch
        {
            foreach (var certificate in collection)
            {
                certificate.Dispose();
            }

            throw;
        }

        if (collection.Count == 0)
        {
            throw new DomeMqConfigurationException([$"{label} did not contain a certificate."]);
        }

        return collection;
    }

    private static LoadedCertificate ParsePem(string id, string certificatePem, string keyPem, string? password)
    {
        if (keyPem.Contains("BEGIN ENCRYPTED PRIVATE KEY", StringComparison.Ordinal) && string.IsNullOrEmpty(password))
        {
            throw new DomeMqConfigurationException([$"Key '{id}' is encrypted. Set Password to an Env or File source."]);
        }

        var combined = string.IsNullOrEmpty(password)
            ? X509Certificate2.CreateFromPem(certificatePem, keyPem)
            : X509Certificate2.CreateFromEncryptedPem(certificatePem, keyPem, password);
        var persisted = Persist(combined);
        if (!persisted.HasPrivateKey)
        {
            persisted.Dispose();
            throw new DomeMqConfigurationException([$"Key '{id}' does not contain a private key."]);
        }

        var imported = new X509Certificate2Collection();
        imported.ImportFromPem(certificatePem);
        var chain = new X509Certificate2Collection();
        foreach (var extra in imported)
        {
            if (!extra.Thumbprint.Equals(persisted.Thumbprint, StringComparison.OrdinalIgnoreCase))
            {
                chain.Add(extra);
            }
            else
            {
                extra.Dispose();
            }
        }

        return new LoadedCertificate(persisted, chain);
    }

    private static X509Certificate2 Persist(X509Certificate2 certificate)
    {
        try
        {
            var exported = certificate.Export(X509ContentType.Pkcs12);
            return X509CertificateLoader.LoadPkcs12(exported, password: null);
        }
        finally
        {
            certificate.Dispose();
        }
    }

    private static byte[]? TryPkcs12(LoadedMaterial material)
    {
        if (material.IsPem)
        {
            return null;
        }

        if (material.Bytes.Length > 1 && material.Bytes[0] == 0x30)
        {
            return material.Bytes;
        }

        var text = material.Text.Trim();
        if (text.Length == 0)
        {
            return null;
        }

        try
        {
            return Convert.FromBase64String(text);
        }
        catch (FormatException)
        {
            return null;
        }
    }
}
