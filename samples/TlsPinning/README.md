# TlsPinning

Generates a local certificate with `KeyGenerator`, serves it as `Tls.KeyMode` `PerServer`, and connects with an SPKI SHA-256 pin. A pin of a different key is rejected. The private CA is not in the machine trust store, so the client sets `AllowUntrustedCertificates` and relies on the pin.

```bash
dotnet run --project samples/TlsPinning
```

The listing in [docs/examples.md](../../docs/examples.md) is this program. Key files are described in [docs/keys.md](../../docs/keys.md).
