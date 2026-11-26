using System.Security.Cryptography.X509Certificates;

namespace DomeMQ.Server;

internal static class CertificateLoader
{
    public static X509Certificate2 Load(TlsOptions tls)
    {
        if (!string.IsNullOrWhiteSpace(tls.PfxPath))
        {
            return X509CertificateLoader.LoadPkcs12FromFile(tls.PfxPath, tls.PfxPassword);
        }

        if (!string.IsNullOrWhiteSpace(tls.KeyPassword))
        {
            return X509Certificate2.CreateFromEncryptedPemFile(tls.CertificatePath!, tls.KeyPassword, tls.KeyPath);
        }

        return X509Certificate2.CreateFromPemFile(tls.CertificatePath!, tls.KeyPath);
    }
}
