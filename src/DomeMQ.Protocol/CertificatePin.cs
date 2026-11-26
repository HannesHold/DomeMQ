using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace DomeMQ;

/// <summary>Compares a server certificate with a pinned SPKI hash or a pinned certificate.</summary>
public static class CertificatePin
{
    public static byte[] SpkiSha256(X509Certificate2 certificate) =>
        SHA256.HashData(certificate.PublicKey.ExportSubjectPublicKeyInfo());

    public static string SpkiSha256Hex(X509Certificate2 certificate) =>
        Convert.ToHexString(SpkiSha256(certificate)).ToLowerInvariant();

    public static bool TryParseSpkiSha256(string text, out byte[] hash, out string error)
    {
        hash = [];
        var compact = new string(text.Where(static c => !char.IsWhiteSpace(c) && c != ':').ToArray());
        if (compact.Length == 64 && compact.All(static c => Uri.IsHexDigit(c)))
        {
            hash = Convert.FromHexString(compact);
            error = "";
            return true;
        }

        try
        {
            var decoded = Convert.FromBase64String(compact);
            if (decoded.Length == 32)
            {
                hash = decoded;
                error = "";
                return true;
            }
        }
        catch (FormatException)
        {
        }

        error = "SPKI SHA-256 must be 32 bytes, as hex or base64.";
        return false;
    }

    public static bool AcceptServer(
        X509Certificate? certificate,
        SslPolicyErrors errors,
        bool allowUntrusted,
        IReadOnlyList<byte[]>? pinnedSpkiSha256,
        X509Certificate2? pinnedCertificate,
        out string failure)
    {
        failure = "";
        if (certificate is null)
        {
            failure = "The server did not present a certificate.";
            return false;
        }

        using var server = new X509Certificate2(certificate);
        if (pinnedSpkiSha256 is { Count: > 0 })
        {
            var actual = SpkiSha256(server);
            var match = false;
            foreach (var hash in pinnedSpkiSha256)
            {
                if (hash.AsSpan().SequenceEqual(actual))
                {
                    match = true;
                    break;
                }
            }

            if (!match)
            {
                failure = "The server certificate does not match any pinned SPKI SHA-256.";
                return false;
            }
        }

        if (pinnedCertificate is not null
            && !string.Equals(server.Thumbprint, pinnedCertificate.Thumbprint, StringComparison.OrdinalIgnoreCase))
        {
            failure = "The server certificate does not match the pinned certificate.";
            return false;
        }

        if (allowUntrusted || errors == SslPolicyErrors.None)
        {
            return true;
        }

        failure = "The server certificate was not trusted.";
        return false;
    }
}
