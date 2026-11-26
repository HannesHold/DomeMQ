# ChannelKinds

Creates a queue, a stack, and a bag on an embedded broker. It checks FIFO and LIFO order, that a batch returns what is already queued, that a zero timeout on an empty channel returns nothing, and that a second connection may push but may not pop. A bag is checked as a set.

```bash
dotnet run --project samples/ChannelKinds
```

The listing in [docs/examples.md](../../docs/examples.md) is this program. Kind semantics are in [docs/channels.md](../../docs/channels.md).
