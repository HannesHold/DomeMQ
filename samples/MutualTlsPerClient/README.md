# MutualTlsPerClient

Generates two server names and one client certificate. The broker uses `Tls.KeyMode` `PerClient`, so `ServerName` (SNI) selects the certificate, and it requires a client certificate signed by the generated CA. The client pins both server SPKI hashes, presents `worker.crt`, and exchanges one queue message. A pin that does not include the selected key is rejected, as is a client with no certificate.

```bash
dotnet run --project samples/MutualTlsPerClient
```

The listing in [docs/examples.md](../../docs/examples.md) is this program.
