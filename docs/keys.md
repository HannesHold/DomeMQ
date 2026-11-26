# TLS keys

DomeMQ can present one server certificate to every connection, or a different certificate per client. The choice is `Tls.KeyMode`.

| Mode | What the server does |
| --- | --- |
| `PerServer` | Every connection on a listener gets `Tls.KeyId`. This is the default. Omit `KeyId` only when `Keys` has exactly one entry. |
| `PerClient` | The TLS server name (SNI) selects the key. Each `Keys` entry has `ServerName` or `ServerNames`. An unknown or missing name is rejected unless `FallbackKeyId` is set. |

Names are compared without case. The same name on two keys fails startup. `PerClient` does not use `KeyId`.

Clients set the SNI name and can pin the server's public key or certificate. A mismatch fails the handshake. Pinning works together with a client certificate for mTLS.

Material is never pasted into JSON. Each source is `{ "Env": "VAR" }` or `{ "File": "/path" }`. `Env` holds PEM text or base64 PKCS#12. `File` points at a PEM or PFX file. An encrypted PEM key or a PFX needs `Password`, also as Env or File.

The broker re-reads key files, names, the mode, and the fallback about every 5 seconds. New handshakes use the new material. Connections that already finished the handshake keep the certificate they negotiated. Turning TLS on for a listener that was bound in plaintext, or changing the bind address, still needs a restart.

## Which file goes where

| File | Who keeps it |
| --- | --- |
| `ca.key` | The person who issues certificates. Never copy it to the broker host or to a client. |
| `ca.crt` | The server, as `Tls.ClientAuthorities`, when clients use certificates signed by this CA. Clients that verify the chain also keep it. |
| `servers/<name>.crt` | The server, as that key's `Certificate`. The PEM includes the leaf and then the CA. |
| `servers/<name>.key` | The server, as that key's `PrivateKey`. |
| `servers/<name>.pfx` | The server, as both `PrivateKey` and `Certificate`, when you chose PFX. |
| `servers/<name>.spki.sha256` | Each client that pins this server. It is the hex SHA-256 of the leaf SubjectPublicKeyInfo. It is not a secret. |
| `clients/<name>.crt` and `clients/<name>.key` | Only that client, as its mTLS certificate and key. |

## Create keys with the broker

`keygen` uses `System.Security.Cryptography`. It does not shell out to OpenSSL.

```bash
dotnet run --project src/DomeMQ.Broker -- keygen \
  --out keys \
  --server alpha.dome,beta.dome \
  --client worker \
  --format pem \
  --days 825
```

A published broker is the same command:

```bash
./DomeMQ.Broker keygen --out /etc/domemq/keys --server alpha.dome --client worker --password 'change-me'
```

| Argument | Meaning |
| --- | --- |
| `--out` | Directory to create. Required. |
| `--server` | SNI name. Repeat the flag, or pass a comma-separated list. The DNS SAN is this name. Required. |
| `--client` | mTLS client name. Optional. Repeat or use commas. |
| `--password` | Encrypts PEM private keys and PFX files. Optional. |
| `--format` | `pem` (default), `pfx`, or `both`. |
| `--days` | Lifetime. Default 825. |
| `--bits` | RSA size: 2048 (default), 3072, or 4096. |

`PerClient` configuration for that output:

```json
{
  "Keys": [
    {
      "Id": "alpha",
      "ServerName": "alpha.dome",
      "PrivateKey": { "File": "/etc/domemq/keys/servers/alpha.dome.key" },
      "Certificate": { "File": "/etc/domemq/keys/servers/alpha.dome.crt" }
    },
    {
      "Id": "beta",
      "ServerName": "beta.dome",
      "PrivateKey": { "File": "/etc/domemq/keys/servers/beta.dome.key" },
      "Certificate": { "File": "/etc/domemq/keys/servers/beta.dome.crt" }
    }
  ],
  "Tls": {
    "Enabled": true,
    "KeyMode": "PerClient",
    "RequireClientCertificate": true,
    "ClientAuthorities": [
      { "File": "/etc/domemq/keys/ca.crt" }
    ]
  }
}
```

A .NET client for `alpha.dome`:

```csharp
await DomeMqClient.ConnectAsync(new DomeMqClientOptions
{
    Host = "10.0.0.8",
    Port = 5071,
    Tls = new TlsClientOptions
    {
        Enabled = true,
        ServerName = "alpha.dome",
        CertificatePath = "worker.crt",
        KeyPath = "worker.key",
        Pin = new ServerPinOptions
        {
            SpkiSha256 = File.ReadAllText("alpha.dome.spki.sha256").Trim()
        }
    }
});
```

`ServerName` is the SNI value. `TargetHost` is the older name for the same field; `ServerName` wins when both are set. `Pin` accepts an SPKI SHA-256 (hex or base64, or raw 32 bytes in `SpkiSha256Bytes`) and a certificate (`CertificateFile`, `CertificateEnv`, or `CertificateBytes`). Every value that is set must match. A certificate pin compares thumbprints, so a reissued certificate fails even when the key is unchanged. An SPKI pin follows the key across reissue.

`AllowUntrustedCertificates` skips chain and name checks. The pin is still enforced. Leave it false when the client trusts `ca.crt`.

The Inspector connect screen has the SNI name, the SPKI hash, and a pinned certificate path next to the client certificate fields. The server dashboard shows the key id chosen for each connection.

## Create keys with OpenSSL

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout ca.key -out ca.crt -days 825 -subj "/CN=DomeMQ CA"

openssl req -newkey rsa:2048 -nodes \
  -keyout alpha.key -out alpha.csr -subj "/CN=alpha.dome"

cat > alpha.ext <<'EOF'
subjectAltName=DNS:alpha.dome
extendedKeyUsage=serverAuth
basicConstraints=CA:FALSE
EOF

openssl x509 -req -in alpha.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out alpha.crt -days 825 -extfile alpha.ext

cat alpha.crt ca.crt > alpha.full.crt

openssl req -newkey rsa:2048 -nodes \
  -keyout worker.key -out worker.csr -subj "/CN=worker"
cat > worker.ext <<'EOF'
extendedKeyUsage=clientAuth
basicConstraints=CA:FALSE
EOF
openssl x509 -req -in worker.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out worker.crt -days 825 -extfile worker.ext

openssl pkcs8 -topk8 -v2 aes-256-cbc -in alpha.key -out alpha.enc.key
openssl pkcs12 -export -inkey alpha.key -in alpha.crt -certfile ca.crt -out alpha.pfx

openssl x509 -in alpha.crt -pubkey -noout \
  | openssl pkey -pubin -outform der \
  | openssl dgst -sha256 -hex
```

Give `alpha.full.crt` and `alpha.key` (or `alpha.pfx`) to the server. Give `ca.crt` to the server only as `ClientAuthorities`. Give `worker.crt` and `worker.key` to that client. Give the SPKI hash, not `ca.key`, to clients that pin the server.

## Create keys with PowerShell

This uses the .NET certificate types. It runs in PowerShell 7 (`pwsh`), which hosts the same runtime as DomeMQ. Windows PowerShell 5.1 does not include these types; use `keygen` there instead.

```powershell
$out = "keys"
New-Item -ItemType Directory -Force -Path $out, "$out/servers", "$out/clients" | Out-Null

function New-Key($bits = 2048) { [System.Security.Cryptography.RSA]::Create($bits) }

function Export-PemCert($cert, $path) {
    [System.IO.File]::WriteAllText($path, $cert.ExportCertificatePem())
}

$caKey = New-Key
$caReq = [System.Security.Cryptography.X509Certificates.CertificateRequest]::new(
    "CN=DomeMQ CA", $caKey,
    [System.Security.Cryptography.HashAlgorithmName]::SHA256,
    [System.Security.Cryptography.RSASignaturePadding]::Pkcs1)
$caReq.CertificateExtensions.Add(
    [System.Security.Cryptography.X509Certificates.X509BasicConstraintsExtension]::new($true, $false, 0, $true))
$ca = $caReq.CreateSelfSigned([datetimeoffset]::UtcNow.AddMinutes(-5), [datetimeoffset]::UtcNow.AddDays(825))
Export-PemCert $ca "$out/ca.crt"
[System.IO.File]::WriteAllText("$out/ca.key", $caKey.ExportPkcs8PrivateKeyPem())

function New-Leaf($name, $server) {
    $key = New-Key
    $req = [System.Security.Cryptography.X509Certificates.CertificateRequest]::new(
        "CN=$name", $key,
        [System.Security.Cryptography.HashAlgorithmName]::SHA256,
        [System.Security.Cryptography.RSASignaturePadding]::Pkcs1)
    $req.CertificateExtensions.Add(
        [System.Security.Cryptography.X509Certificates.X509BasicConstraintsExtension]::new($false, $false, 0, $true))
    $oid = $server ? "1.3.6.1.5.5.7.3.1" : "1.3.6.1.5.5.7.3.2"
    $req.CertificateExtensions.Add(
        [System.Security.Cryptography.X509Certificates.X509EnhancedKeyUsageExtension]::new(
            @([System.Security.Cryptography.Oid]::new($oid)), $false))
    if ($server) {
        $san = [System.Security.Cryptography.X509Certificates.SubjectAlternativeNameBuilder]::new()
        $san.AddDnsName($name)
        $req.CertificateExtensions.Add($san.Build())
    }
    $issued = $req.Create($ca, [datetimeoffset]::UtcNow.AddMinutes(-5), [datetimeoffset]::UtcNow.AddDays(825),
        [byte[]](1,2,3,4,5,6,7,8))
    $leaf = $issued.CopyWithPrivateKey($key)
    $dir = $server ? "$out/servers" : "$out/clients"
    $pem = $leaf.ExportCertificatePem()
    if ($server) { $pem += $ca.ExportCertificatePem() }
    [System.IO.File]::WriteAllText("$dir/$name.crt", $pem)
    [System.IO.File]::WriteAllText("$dir/$name.key", $key.ExportPkcs8PrivateKeyPem())
    $spki = [System.Security.Cryptography.SHA256]::HashData($leaf.PublicKey.ExportSubjectPublicKeyInfo())
    [System.IO.File]::WriteAllText("$dir/$name.spki.sha256", ([Convert]::ToHexString($spki).ToLowerInvariant() + "`n"))
}

New-Leaf "alpha.dome" $true
New-Leaf "worker" $false
```

## Environment variables and Docker

PEM in an environment variable uses real newlines, or the two-character sequence `\n` when the value is one line.

```json
{
  "Id": "alpha",
  "ServerName": "alpha.dome",
  "PrivateKey": { "Env": "DOMEMQ_ALPHA_KEY" },
  "Certificate": { "Env": "DOMEMQ_ALPHA_CERT" },
  "Password": { "Env": "DOMEMQ_ALPHA_PASSWORD" }
}
```

Docker secrets stay files. Mount them and point `File` at the mount. Do not put `ca.key` in the image.

```yaml
services:
  domemq:
    image: ghcr.io/hanneshold/domemq:0.1.0
    secrets:
      - alpha_key
      - alpha_cert
    volumes:
      - ./domemq.json:/config/domemq.json:ro
secrets:
  alpha_key:
    file: ./keys/servers/alpha.dome.key
  alpha_cert:
    file: ./keys/servers/alpha.dome.crt
```

```json
"PrivateKey": { "File": "/run/secrets/alpha_key" },
"Certificate": { "File": "/run/secrets/alpha_cert" }
```

## Rotation

`PerServer`: add the new pair under a new `Id`, deploy the files, then change `KeyId`. Or overwrite the files of the current id. The next reload picks them up for new connections.

`PerClient`: give the new certificate the same `ServerName` by replacing that entry's files, or add a new id, move `ServerName` onto it, and delete the old name from the old entry in the same edit. Two keys must not own the name at once.

Clients that pin an SPKI keep working when the certificate is reissued with the same key. They fail when the key changes, until their pin is updated. Clients that pin the certificate thumbprint must be updated for every reissue. Ship the new `*.spki.sha256` or `*.crt` to those clients before, or together with, the server change. There is no second pin slot.

`ca.key` stays offline. `ca.crt` changes only when you replace the authority, which also means reissuing every certificate it signed.
