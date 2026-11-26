using System.Buffers;
using System.Text.Json;

namespace DomeMQ;

/// <summary>
/// Application payload carried by Push and Pop. DomeMQ stores and forwards the bytes from
/// <see cref="GetPayload"/> and does not interpret them. Implementations live in the caller's process.
/// </summary>
public interface IPayload
{
    /// <summary>Expected encoded size, when known without encoding. Null if the size is not cheap to compute.</summary>
    int? SizeHint { get; }

    ReadOnlyMemory<byte> GetPayload();

    void SetPayload(ReadOnlySpan<byte> data);
}

/// <summary>
/// Optional zero-copy write. The client uses this when the payload implements it;
/// otherwise it calls <see cref="IPayload.GetPayload"/>.
/// </summary>
public interface IWritablePayload : IPayload
{
    /// <summary>Writes the encoded payload and returns the number of bytes written.</summary>
    int WriteTo(IBufferWriter<byte> writer);
}

/// <summary>
/// Adapter a caller can implement to plug in MessagePack, Protobuf, or any other serializer
/// without DomeMQ taking a dependency on that library.
/// </summary>
public interface IPayloadSerializer<T>
{
    void Serialize(T value, IBufferWriter<byte> destination);

    T Deserialize(ReadOnlySpan<byte> source);
}

/// <summary>Opaque application bytes.</summary>
public sealed class RawPayload : IPayload
{
    private byte[] _data;

    public RawPayload() => _data = [];

    public RawPayload(ReadOnlyMemory<byte> data) => _data = data.ToArray();

    public int? SizeHint => _data.Length;

    public ReadOnlyMemory<byte> Bytes => _data;

    public ReadOnlyMemory<byte> GetPayload() => _data;

    public void SetPayload(ReadOnlySpan<byte> data) => _data = data.ToArray();
}

/// <summary>UTF-8 text. The broker still sees only the encoded bytes.</summary>
public sealed class Utf8StringPayload : IPayload
{
    public Utf8StringPayload()
    {
    }

    public Utf8StringPayload(string text) => Text = text;

    public string Text { get; set; } = "";

    public int? SizeHint => ProtocolEncoding.Utf8.GetByteCount(Text);

    public ReadOnlyMemory<byte> GetPayload() => ProtocolEncoding.Utf8.GetBytes(Text);

    public void SetPayload(ReadOnlySpan<byte> data) => Text = ProtocolEncoding.Utf8.GetString(data);
}

/// <summary>
/// JSON object encoded with <see cref="JsonSerializer"/> (BCL). DomeMQ does not parse the result;
/// the bytes are an ordinary application payload.
/// </summary>
public sealed class JsonPayload<T> : IPayload
{
    public JsonPayload()
    {
    }

    public JsonPayload(T value) => Value = value;

    public T? Value { get; set; }

    public JsonSerializerOptions? Options { get; set; }

    public int? SizeHint => null;

    public ReadOnlyMemory<byte> GetPayload() => JsonSerializer.SerializeToUtf8Bytes(Value, Options);

    public void SetPayload(ReadOnlySpan<byte> data) => Value = JsonSerializer.Deserialize<T>(data, Options);
}
