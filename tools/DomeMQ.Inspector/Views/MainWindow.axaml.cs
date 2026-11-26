using Avalonia.Controls;
using DomeMQ.Inspector.ViewModels;

namespace DomeMQ.Inspector.Views;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
        Opened += (_, _) =>
        {
            if (DataContext is ShellViewModel shell)
            {
                shell.Activate();
            }
        };
    }
}
