using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using DomeMQ.Inspector.ViewModels;
using DomeMQ.Inspector.Views;

namespace DomeMQ.Inspector;

public partial class App : Application
{
    public override void Initialize() => AvaloniaXamlLoader.Load(this);

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            desktop.MainWindow = new MainWindow
            {
                DataContext = new ShellViewModel()
            };
        }

        base.OnFrameworkInitializationCompleted();
    }
}
