using Avalonia.Controls;
using DomeMQ.Inspector.ViewModels;

namespace DomeMQ.Inspector.Views;

public partial class SendView : UserControl
{
    private bool _styled;

    public SendView()
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

    public void RefreshPlot()
    {
        if (!IsVisible || DataContext is not ClientViewModel viewModel)
        {
            return;
        }

        Plot.Plot.Clear();
        if (viewModel.StressRates.Length > 0)
        {
            var signal = Plot.Plot.Add.Signal(viewModel.StressRates);
            signal.Color = ScottPlot.Color.FromHex("#3DDC97");
            Plot.Plot.Axes.AutoScale();
            var limits = Plot.Plot.Axes.GetLimits();
            var pad = Math.Max(1, (limits.Top - limits.Bottom) * 0.25);
            Plot.Plot.Axes.SetLimitsY(limits.Bottom - pad, limits.Top + pad);
        }

        Plot.Plot.Axes.Left.Label.Text = "msgs/s";
        Plot.Refresh();
    }
}
