namespace DomeMQ;

/// <summary>
/// Constants of protocol version 1. Field layouts live in <c>docs/protocol.md</c>.
/// </summary>
public static class ProtocolConstants
{
    /// <summary>Current protocol version written in the header and in <c>Connect</c>.</summary>
    public const byte Version = 1;

    /// <summary>Fixed header size in bytes.</summary>
    public const int HeaderSize = 16;

    /// <summary>Magic bytes <c>44 4D 51 01</c> ("DMQ" followed by 0x01).</summary>
    public static ReadOnlySpan<byte> Magic => MagicBytes;

    private static readonly byte[] MagicBytes = [0x44, 0x4D, 0x51, 0x01];

    public const int MaxChannelNameBytes = 256;
    public const int MaxClientNameBytes = 256;
    public const int MaxServerNameBytes = 256;
    public const int MaxAuthTokenBytes = 1024;
    public const int MaxContentTypeBytes = 256;
    public const int MaxErrorMessageBytes = 1024;
    public const int MaxReasonBytes = 1024;
    public const int MaxSessionIdBytes = 64;
    public const int MaxPopBatch = 1024;

    /// <summary>Largest <c>MaxPayloadSize</c> a server will accept in configuration (256 MiB).</summary>
    public const int MaxConfigurablePayload = 256 * 1024 * 1024;

    /// <summary>
    /// Bytes added to <c>MaxPayloadSize</c> when deciding the largest frame body to allocate.
    /// Covers channel names, content-type labels, and length prefixes around one application body.
    /// </summary>
    public const int FrameEnvelopeReserve = 64 * 1024;

    /// <summary>
    /// Inbound frames other than <c>Push</c> are capped at this size so a huge <c>Connect</c>
    /// cannot force a multi-megabyte allocation. <c>Push</c> uses the payload wire cap.
    /// </summary>
    public const int ControlFrameCap = 256 * 1024;
}
