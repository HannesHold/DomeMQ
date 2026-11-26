namespace DomeMQ;

/// <summary>Base exception for DomeMQ protocol and client failures.</summary>
public class DomeMqException : Exception
{
    public ErrorCode? Code { get; }

    public DomeMqException(string message, ErrorCode? code = null, Exception? inner = null)
        : base(message, inner)
    {
        Code = code;
    }
}

/// <summary>A frame or message body is not valid version-1 protocol.</summary>
public sealed class ProtocolException : DomeMqException
{
    public ProtocolException(string message)
        : base(message, ErrorCode.ProtocolError)
    {
    }
}

/// <summary>
/// An application payload, or a frame that would have to be allocated for one,
/// exceeds the effective payload limit.
/// </summary>
public sealed class PayloadTooLargeException : DomeMqException
{
    /// <summary>Byte count that was rejected. <c>-1</c> when the server reported the error without sizes.</summary>
    public long ActualSize { get; }

    /// <summary>Limit that was exceeded. <c>-1</c> when the server reported the error without sizes.</summary>
    public long Limit { get; }

    public PayloadTooLargeException(long actualSize, long limit)
        : base($"Payload of {actualSize} bytes exceeds the limit of {limit} bytes.", ErrorCode.PayloadTooLarge)
    {
        ActualSize = actualSize;
        Limit = limit;
    }

    public PayloadTooLargeException(string message)
        : base(message, ErrorCode.PayloadTooLarge)
    {
        ActualSize = -1;
        Limit = -1;
    }
}

/// <summary>The named transport has no implementation yet.</summary>
public sealed class TransportNotSupportedException : DomeMqException
{
    public TransportNotSupportedException(string message)
        : base(message)
    {
    }
}
