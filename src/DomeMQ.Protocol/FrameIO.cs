namespace DomeMQ;

/// <summary>Reads and writes whole frames on a byte transport.</summary>
public static class FrameIO
{
    public static ValueTask<Frame> ReadAsync(
        IByteTransport transport,
        int maxFrameBody,
        TimeSpan firstByteTimeout,
        TimeSpan readTimeout,
        CancellationToken cancellationToken) =>
        ReadAsync(transport, _ => maxFrameBody, firstByteTimeout, readTimeout, cancellationToken);

    /// <summary>
    /// Reads one frame. <paramref name="maxFrameBody"/> is evaluated after the header is parsed
    /// and before the body is allocated, so the cap can depend on the message type.
    /// </summary>
    public static async ValueTask<Frame> ReadAsync(
        IByteTransport transport,
        Func<FrameHeader, int> maxFrameBody,
        TimeSpan firstByteTimeout,
        TimeSpan readTimeout,
        CancellationToken cancellationToken)
    {
        var header = new byte[ProtocolConstants.HeaderSize];
        await ReadExactAsync(transport, header.AsMemory(0, 1), firstByteTimeout, cancellationToken).ConfigureAwait(false);
        await ReadExactAsync(transport, header.AsMemory(1), readTimeout, cancellationToken).ConfigureAwait(false);
        if (!FrameCodec.TryReadHeader(header, out var parsed, out var error))
        {
            throw new ProtocolException(error);
        }

        var cap = maxFrameBody(parsed);
        if (parsed.PayloadLength > (uint)cap)
        {
            throw new PayloadTooLargeException(parsed.PayloadLength, cap);
        }

        var body = new byte[parsed.PayloadLength];
        if (body.Length > 0)
        {
            await ReadExactAsync(transport, body, readTimeout, cancellationToken).ConfigureAwait(false);
        }

        return new Frame(parsed.Version, parsed.Type, parsed.Flags, parsed.RequestId, body);
    }

    public static async ValueTask WriteAsync(
        IByteTransport transport,
        Frame frame,
        TimeSpan writeTimeout,
        CancellationToken cancellationToken)
    {
        var bytes = FrameCodec.Encode(frame);
        using var timeout = CreateTimeout(writeTimeout, cancellationToken);
        try
        {
            await transport.SendAsync(bytes, timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException("Timed out while writing a frame.");
        }
    }

    public static async ValueTask ReadExactAsync(
        IByteTransport transport,
        Memory<byte> buffer,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        using var linked = CreateTimeout(timeout, cancellationToken);
        var read = 0;
        try
        {
            while (read < buffer.Length)
            {
                var count = await transport.ReceiveAsync(buffer[read..], linked.Token).ConfigureAwait(false);
                if (count == 0)
                {
                    throw new EndOfStreamException("Connection closed.");
                }

                read += count;
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException("Timed out while reading a frame.");
        }
    }

    private static CancellationTokenSource CreateTimeout(TimeSpan timeout, CancellationToken cancellationToken)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (timeout > TimeSpan.Zero)
        {
            source.CancelAfter(timeout);
        }

        return source;
    }
}
