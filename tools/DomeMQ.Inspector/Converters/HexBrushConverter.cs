using System.Globalization;
using Avalonia.Data.Converters;
using Avalonia.Media;

namespace DomeMQ.Inspector.Converters;

public sealed class HexBrushConverter : IValueConverter
{
    public static readonly HexBrushConverter Instance = new();

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        value is true ? SolidColorBrush.Parse("#8AB4FF") : SolidColorBrush.Parse("#F0C674");

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}
