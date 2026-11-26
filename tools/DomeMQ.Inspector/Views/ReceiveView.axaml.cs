using Avalonia.Controls;
using DomeMQ.Inspector.ViewModels;

namespace DomeMQ.Inspector.Views;

public partial class ReceiveView : UserControl
{
    private bool _styled;

    public ReceiveView()
    {
        InitializeComponent();
        DataContextChanged += (_, _) =>
        {
            if (DataContext is not ClientViewModel viewModel)
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
        };
    }

    private void RefreshPlot()
    {
        if (!IsVisible || DataContext is not ClientViewModel viewModel)
        {
            return;
        }

        Plot.Plot.Clear();
        if (viewModel.ReceiveRates.Length > 0)
        {
            var signal = Plot.Plot.Add.Signal(viewModel.ReceiveRates);
            signal.Color = ScottPlot.Color.FromHex("#F0C674");
            Plot.Plot.Axes.AutoScale();
        }

        Plot.Plot.Axes.Left.Label.Text = "msgs/s";
        Plot.Refresh();
    }
}
