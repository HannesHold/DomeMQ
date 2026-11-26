namespace DomeMQ.Inspector.Services;

public sealed class HexCell
{
    public string Text { get; init; } = "";

    public bool IsHeader { get; init; }

    public bool IsPayload { get; init; }
}

public sealed class HexRow
{
    public string Offset { get; init; } = "";

    public string Ascii { get; init; } = "";

    public IReadOnlyList<HexCell> Cells { get; init; } = [];
}

public static class HexFormat
{
    public const int DetailCap = 4096;

    public static IReadOnlyList<HexRow> Rows(ReadOnlySpan<byte> data, int headerLength)
    {
        var shown = Math.Min(data.Length, DetailCap);
        var rows = new List<HexRow>((shown + 15) / 16);
        for (var offset = 0; offset < shown; offset += 16)
        {
            var count = Math.Min(16, shown - offset);
            var cells = new List<HexCell>(count);
            var ascii = new char[count];
            for (var i = 0; i < count; i++)
            {
                var index = offset + i;
                var value = data[index];
                var header = index < headerLength;
                cells.Add(new HexCell
                {
                    Text = value.ToString("X2"),
                    IsHeader = header,
                    IsPayload = !header
                });
                ascii[i] = value is >= 32 and <= 126 ? (char)value : '.';
            }

            rows.Add(new HexRow
            {
                Offset = offset.ToString("X4"),
                Ascii = new string(ascii),
                Cells = cells
            });
        }

        return rows;
    }

    public static byte[] ParsePayload(string text, bool hex)
    {
        if (!hex)
        {
            return System.Text.Encoding.UTF8.GetBytes(text);
        }

        var digits = new string(text.Where(static c => Uri.IsHexDigit(c)).ToArray());
        if (digits.Length % 2 != 0)
        {
            throw new FormatException("Hex payload needs an even number of digits.");
        }

        var bytes = new byte[digits.Length / 2];
        for (var i = 0; i < bytes.Length; i++)
        {
            bytes[i] = Convert.ToByte(digits.Substring(i * 2, 2), 16);
        }

        return bytes;
    }
}
