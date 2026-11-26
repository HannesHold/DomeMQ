namespace DomeMQ;

/// <summary>Frame message type. Values are stable for protocol version 1.</summary>
public enum MessageType : byte
{
    Connect = 1,
    Hello = 2,
    CreateChannel = 3,
    DeleteChannel = 4,
    Push = 5,
    Pop = 6,
    Ack = 7,
    Error = 8,
    Ping = 9,
    Pong = 10,
    Disconnect = 11
}

/// <summary>How a channel orders messages.</summary>
public enum ChannelKind : byte
{
    /// <summary>First in, first out.</summary>
    Queue = 1,

    /// <summary>Last in, first out.</summary>
    Stack = 2,

    /// <summary>Unordered. Receivers must not depend on pop order.</summary>
    Bag = 3
}

/// <summary>Error codes carried by <c>Error</c> frames. Values are stable for version 1.</summary>
public enum ErrorCode : ushort
{
    Unspecified = 0,
    ProtocolError = 1,
    UnsupportedVersion = 2,
    Unauthorized = 3,
    ChannelNotFound = 4,
    ChannelExists = 5,
    NotChannelOwner = 6,
    InvalidChannelType = 7,
    PayloadTooLarge = 8,
    ChannelFull = 9,
    InvalidMessage = 10,
    NotConnected = 11,
    InternalError = 12,
    TooManyChannels = 13,
    TooManyClients = 14,
    InvalidPayloadLimit = 15,
    InvalidQueueLimit = 16
}

/// <summary>Reason codes carried by <c>Disconnect</c>.</summary>
public enum DisconnectReason : ushort
{
    Normal = 0,
    ProtocolError = 1,
    Shutdown = 2,
    IdleTimeout = 3,
    HandshakeTimeout = 4,
    Unauthorized = 5,
    TooManyClients = 6,
    PayloadTooLarge = 7
}

/// <summary>Header flag bits. Bits 0–7 are must-understand. Bits 8–15 may be ignored.</summary>
public static class FrameFlags
{
    public const ushort None = 0;

    /// <summary>
    /// The sender does not want a success <c>Ack</c>. Honored for <c>Push</c>.
    /// Errors are still returned. Other requests are acknowledged anyway.
    /// </summary>
    public const ushort NoAck = 1 << 0;

    public const ushort MustUnderstandMask = 0x00FF;
    public const ushort KnownMustUnderstand = NoAck;

    public static bool HasUnknownMustUnderstand(ushort flags) =>
        (flags & MustUnderstandMask & ~KnownMustUnderstand) != 0;
}
