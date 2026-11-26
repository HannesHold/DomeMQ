# OverflowPolicies

Embeds a few small brokers and checks the overflow rules: `DiscardNewest` (ack status discarded), `Reject`, `DiscardOldest`, and `ClearAll`; create errors `InvalidQueueLimit` (16) and `InvalidByteLimit` (19); push errors `ClientQueueLimit` (17) and `ServerMemoryCap` (18); `QueuePressure`; and `GetStatistics`.

```bash
dotnet run --project samples/OverflowPolicies
```

The listing in [docs/examples.md](../../docs/examples.md) is this program.
