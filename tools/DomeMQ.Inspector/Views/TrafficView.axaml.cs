using Avalonia.Controls;
using DomeMQ.Inspector.ViewModels;

namespace DomeMQ.Inspector.Views;

public partial class TrafficView : UserControl
{
    public TrafficView()
    {
        InitializeComponent();
        DataContextChanged += (_, _) =>
        {
            if (DataContext is not TrafficViewModel viewModel)
            {
                return;
            }

            viewModel.ScrollRequested -= Scroll;
            viewModel.ScrollRequested += Scroll;
        };
    }

    private void Scroll()
    {
        if (DataContext is TrafficViewModel viewModel && viewModel.Entries.Count > 0)
        {
            FrameList.ScrollIntoView(viewModel.Entries[^1]);
        }
    }
}
