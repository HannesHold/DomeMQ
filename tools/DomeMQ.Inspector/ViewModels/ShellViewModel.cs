using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using DomeMQ.Inspector.Services;

namespace DomeMQ.Inspector.ViewModels;

public partial class ShellViewModel : ObservableObject
{
    private readonly DispatcherTimer _timer;

    public ShellViewModel()
    {
        Log = new TrafficLog();
        Traffic = new TrafficViewModel(Log);
        Server = new ServerViewModel(new ServerHost(), Log);
        Client = new ClientViewModel(Log, Traffic);
        Client.ShowLogRequested += () => Page = "traffic";
        Mode = LaunchOptions.Mode switch
        {
            InspectorMode.Server => "server",
            InspectorMode.Client => "client",
            _ => "choose"
        };
        Page = Mode switch
        {
            "server" => "overview",
            "client" => "connect",
            _ => "start"
        };
        _timer = new DispatcherTimer { Interval = TimeSpan.FromMilliseconds(100) };
        _timer.Tick += (_, _) => Tick();
    }

    public TrafficLog Log { get; }

    public TrafficViewModel Traffic { get; }

    public ServerViewModel Server { get; }

    public ClientViewModel Client { get; }

    [ObservableProperty] private string _mode;

    [ObservableProperty] private string _page;

    public string ModeLabel => Mode switch
    {
        "server" => "Server mode",
        "client" => "Client mode",
        _ => "Choose a mode"
    };

    public bool IsStart => Mode == "choose";
    public bool IsServer => Mode == "server";
    public bool IsClient => Mode == "client";
    public bool IsOverview => IsServer && Page == "overview";
    public bool IsClients => IsServer && Page == "clients";
    public bool IsChannels => IsServer && Page == "channels";
    public bool IsConnect => IsClient && Page == "connect";
    public bool IsSend => IsClient && Page == "send";
    public bool IsReceive => IsClient && Page == "receive";
    public bool IsTests => IsClient && Page == "tests";
    public bool IsTraffic => Page == "traffic";

    public void Activate() => _timer.Start();

    partial void OnModeChanged(string value) => RaisePages();

    partial void OnPageChanged(string value) => RaisePages();

    [RelayCommand]
    private void Choose(string mode)
    {
        Mode = mode;
        Page = mode == "server" ? "overview" : "connect";
    }

    [RelayCommand]
    private void ShowPage(string page) => Page = page;

    private void Tick()
    {
        var batch = Log.Drain(200);
        Traffic.Apply(batch);
        if (IsServer)
        {
            Server.Refresh();
        }

        if (IsClient)
        {
            Client.Refresh();
        }
    }

    private void RaisePages()
    {
        OnPropertyChanged(nameof(ModeLabel));
        OnPropertyChanged(nameof(IsStart));
        OnPropertyChanged(nameof(IsServer));
        OnPropertyChanged(nameof(IsClient));
        OnPropertyChanged(nameof(IsOverview));
        OnPropertyChanged(nameof(IsClients));
        OnPropertyChanged(nameof(IsChannels));
        OnPropertyChanged(nameof(IsConnect));
        OnPropertyChanged(nameof(IsSend));
        OnPropertyChanged(nameof(IsReceive));
        OnPropertyChanged(nameof(IsTests));
        OnPropertyChanged(nameof(IsTraffic));
    }
}
