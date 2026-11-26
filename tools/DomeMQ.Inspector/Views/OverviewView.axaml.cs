using Avalonia.Controls;
using DomeMQ.Inspector.ViewModels;

namespace DomeMQ.Inspector.Views;

public partial class OverviewView : UserControl
{
    private bool _styled;

    public OverviewView()
    {
        InitializeComponent();
        DataContextChanged += (_, _) => Hook();
    }

    private void Hook()
    {
        if (DataContext is not ServerViewModel viewModel)
        {
            return;
        }

        viewModel.ChartUpdated -= RefreshPlot;
        viewModel.ChartUpdated += RefreshPlot;
        if (!_styled)
        {
            Plot.Plot.FigureBackground.Color = ScottPlot.Color.FromHex("#1C2030");
            Plot.Plot.DataBackground.Color = ScottPlot.Color.FromHex("#16181F");
            Plot.Plot.Axes.Color(ScottPlot.Color.FromHex("#8B95A8"));
            _styled = true;
        }
    }

    private void RefreshPlot()
    {
        if (!IsVisible || DataContext is not ServerViewModel viewModel)
        {
            return;
        }

        Plot.Plot.Clear();
        if (viewModel.Rates.Length > 0)
        {
            var signal = Plot.Plot.Add.Signal(viewModel.Rates);
            signal.Color = ScottPlot.Color.FromHex("#6CB6FF");
            Plot.Plot.Axes.AutoScale();
        }

        Plot.Plot.Axes.Left.Label.Text = "msgs/s";
        Plot.Refresh();
    }
}
