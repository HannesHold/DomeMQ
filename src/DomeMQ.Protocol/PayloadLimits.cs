namespace DomeMQ;

/// <summary>
/// Resolves the effective application-payload limit.
/// Unset (null) levels are skipped. The server global limit is always present on the server.
/// Effective limit = min(server global, listener, channel, message) among the levels that are set.
/// </summary>
public static class PayloadLimits
{
    public static int Min(int serverGlobal, int? listener, int? channel, int? message)
    {
        if (serverGlobal <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(serverGlobal));
        }

        var limit = serverGlobal;
        if (listener is int listenerLimit)
        {
            limit = Math.Min(limit, listenerLimit);
        }

        if (channel is int channelLimit)
        {
            limit = Math.Min(limit, channelLimit);
        }

        if (message is int messageLimit)
        {
            limit = Math.Min(limit, messageLimit);
        }

        return limit;
    }

    public static int Min(params int?[] limits)
    {
        int? result = null;
        foreach (var candidate in limits)
        {
            if (candidate is not int value)
            {
                continue;
            }

            result = result is int current ? Math.Min(current, value) : value;
        }

        if (result is null)
        {
            throw new InvalidOperationException("At least one payload limit must be set.");
        }

        return result.Value;
    }

    /// <summary>Largest frame body the broker will allocate for the given application-payload limit.</summary>
    public static int WireCap(int maxPayloadSize) => checked(maxPayloadSize + ProtocolConstants.FrameEnvelopeReserve);

    /// <summary>
    /// Allocation cap for one inbound frame. <c>Push</c> may carry an application body;
    /// every other client message is capped at <see cref="ProtocolConstants.ControlFrameCap"/>.
    /// </summary>
    public static int InboundFrameCap(byte messageType, int maxPayloadSize)
    {
        var wire = WireCap(maxPayloadSize);
        if (messageType == (byte)MessageType.Push)
        {
            return wire;
        }

        return Math.Min(wire, ProtocolConstants.ControlFrameCap);
    }
}
