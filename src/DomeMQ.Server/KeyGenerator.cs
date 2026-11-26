using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace DomeMQ.Server;

public enum KeyGenFormat
{
    Pem,
    Pfx,
    Both
}

public sealed class KeyGenRequest
{
    public required string OutputDirectory { get; init; }

    public required IReadOnlyList<string> ServerNames { get; init; }

    public IReadOnlyList<string> ClientNames { get; init; } = [];

    public string? Password { get; init; }

    public KeyGenFormat Format { get; init; } = KeyGenFormat.Pem;

    public int Days { get; init; } = 825;

    public int RsaBits { get; init; } = 2048;
}

public sealed class KeyGenResult
{
    public required string OutputDirectory { get; init; }

    public required string CaCertificatePath { get; init; }

    public required string CaKeyPath { get; init; }

    public required IReadOnlyList<string> ServerCertificatePaths { get; init; }

    public required IReadOnlyList<string> ClientCertificatePaths { get; init; }
}

/// <summary>
/// Creates a local CA, one server certificate per SNI name, and optional mTLS client certificates.
/// Uses only <see cref="System.Security.Cryptography"/>.
/// </summary>
public static class KeyGenerator
{
    public static KeyGenResult Generate(KeyGenRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrWhiteSpace(request.OutputDirectory))
        {
            throw new ArgumentException("Output directory is required.", nameof(request));
        }

        if (request.ServerNames.Count == 0)
        {
            throw new ArgumentException("At least one server name is required.", nameof(request));
        }

        if (request.Days is < 1 or > 3650)
        {
            throw new ArgumentException("Days must be between 1 and 3650.", nameof(request));
        }

        if (request.RsaBits is not 2048 and not 3072 and not 4096)
        {
            throw new ArgumentException("RSA size must be 2048, 3072, or 4096.", nameof(request));
        }

        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in request.ServerNames.Concat(request.ClientNames))
        {
            if (!IsSafeName(name) || !names.Add(name))
            {
                throw new ArgumentException($"'{name}' is not a usable certificate name. Use letters, digits, '.', '_', and '-'.");
            }
        }

        Directory.CreateDirectory(request.OutputDirectory);
        var servers = Path.Combine(request.OutputDirectory, "servers");
        var clients = Path.Combine(request.OutputDirectory, "clients");
        Directory.CreateDirectory(servers);
        if (request.ClientNames.Count > 0)
        {
            Directory.CreateDirectory(clients);
        }

        var notBefore = DateTimeOffset.UtcNow.AddMinutes(-5);
        var notAfter = notBefore.AddDays(request.Days);
        using var caKey = RSA.Create(request.RsaBits);
        var caRequest = new CertificateRequest("CN=DomeMQ CA", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        caRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        using var caIssued = caRequest.CreateSelfSigned(notBefore, notAfter);
        using var ca = Persist(caIssued);

        var caCertPath = Path.Combine(request.OutputDirectory, "ca.crt");
        var caKeyPath = Path.Combine(request.OutputDirectory, "ca.key");
        File.WriteAllText(caCertPath, ca.ExportCertificatePem());
        WriteKey(caKeyPath, caKey, request.Password);

        var serverPaths = new List<string>();
        foreach (var name in request.ServerNames)
        {
            serverPaths.Add(WriteLeaf(servers, name, ca, request, server: true, notBefore, notAfter));
        }

        var clientPaths = new List<string>();
        foreach (var name in request.ClientNames)
        {
            clientPaths.Add(WriteLeaf(clients, name, ca, request, server: false, notBefore, notAfter));
        }

        return new KeyGenResult
        {
            OutputDirectory = request.OutputDirectory,
            CaCertificatePath = caCertPath,
            CaKeyPath = caKeyPath,
            ServerCertificatePaths = serverPaths,
            ClientCertificatePaths = clientPaths
        };
    }

    private static string WriteLeaf(
        string directory,
        string name,
        X509Certificate2 ca,
        KeyGenRequest request,
        bool server,
        DateTimeOffset notBefore,
        DateTimeOffset notAfter)
    {
        using var key = RSA.Create(request.RsaBits);
        var subject = server ? $"CN={name}" : $"CN={name}";
        var leafRequest = new CertificateRequest(subject, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        leafRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        leafRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        var usage = server ? "1.3.6.1.5.5.7.3.1" : "1.3.6.1.5.5.7.3.2";
        leafRequest.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid(usage)], false));
        if (server)
        {
            var alternative = new SubjectAlternativeNameBuilder();
            alternative.AddDnsName(name);
            leafRequest.CertificateExtensions.Add(alternative.Build());
        }

        var serial = RandomNumberGenerator.GetBytes(8);
        serial[0] &= 0x7F;
        using var issued = leafRequest.Create(ca, notBefore, notAfter, serial);
        using var withKey = issued.CopyWithPrivateKey(key);
        using var persisted = Persist(withKey);

        var certificatePath = Path.Combine(directory, name + ".crt");
        var pem = persisted.ExportCertificatePem();
        if (server)
        {
            pem += ca.ExportCertificatePem();
        }

        if (request.Format is KeyGenFormat.Pem or KeyGenFormat.Both)
        {
            File.WriteAllText(certificatePath, pem);
            WriteKey(Path.Combine(directory, name + ".key"), key, request.Password);
        }

        if (request.Format is KeyGenFormat.Pfx or KeyGenFormat.Both)
        {
            var pfxPath = Path.Combine(directory, name + ".pfx");
            var collection = new X509Certificate2Collection { persisted, ca };
            File.WriteAllBytes(pfxPath, collection.ExportPkcs12(Pkcs12ExportPbeParameters.Pbes2Aes256Sha256, request.Password ?? ""));
            if (request.Format == KeyGenFormat.Pfx)
            {
                certificatePath = pfxPath;
            }
        }

        if (server)
        {
            File.WriteAllText(Path.Combine(directory, name + ".spki.sha256"), CertificatePin.SpkiSha256Hex(persisted) + Environment.NewLine);
        }

        return certificatePath;
    }

    private static void WriteKey(string path, RSA key, string? password)
    {
        var pem = string.IsNullOrEmpty(password)
            ? key.ExportPkcs8PrivateKeyPem()
            : key.ExportEncryptedPkcs8PrivateKeyPem(password, new PbeParameters(PbeEncryptionAlgorithm.Aes256Cbc, HashAlgorithmName.SHA256, 100_000));
        File.WriteAllText(path, pem);
        if (!OperatingSystem.IsWindows())
        {
            File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }
    }

    private static X509Certificate2 Persist(X509Certificate2 certificate)
    {
        var exported = certificate.Export(X509ContentType.Pkcs12);
        return X509CertificateLoader.LoadPkcs12(exported, password: null);
    }

    private static bool IsSafeName(string name) =>
        name.Length is > 0 and <= 128 && name.All(static c => char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or '-');
}
