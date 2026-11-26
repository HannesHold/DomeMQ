namespace DomeMQ;

public enum TrafficDirection
{
    Inbound,
    Outbound
}

/// <summary>
/// One socket read or write. <see cref="Data"/> is a private copy; the transport buffer may be reused.
/// Partial reads and partial sends are reported as separate chunks.
/// </summary>
public readonly record struct TrafficChunk(
    DateTimeOffset Timestamp,
    TrafficDirection Direction,
    string Endpoint,
    byte[] Data);

/// <summary>
/// Tap for the DomeMQ byte stream (the bytes after TLS, when TLS is enabled).
/// Implementations must return quickly and must not throw.
/// </summary>
public interface ITrafficObserver
{
    void OnChunk(TrafficChunk chunk);
}
