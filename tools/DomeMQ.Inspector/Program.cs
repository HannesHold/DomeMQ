using Avalonia;
using DomeMQ.Inspector.Services;

namespace DomeMQ.Inspector;

internal static class Program
{
    [STAThread]
    public static void Main(string[] args)
    {
        LaunchOptions.Parse(args);
        BuildAvaloniaApp().StartWithClassicDesktopLifetime(args);
    }

    public static AppBuilder BuildAvaloniaApp() =>
        AppBuilder.Configure<App>()
            .UsePlatformDetect()
            .WithInterFont()
            .LogToTrace();
}
