using System.Net;
using DomeMQ.Broker;
using DomeMQ.Server;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

if (args.Length > 0 && args[0].Equals("keygen", StringComparison.OrdinalIgnoreCase))
{
    return KeyGenCommand.Run(args.Skip(1).ToArray());
}

var commandArgs = args;
var configPath = TakeConfigPath(ref commandArgs);
var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = commandArgs,
    ContentRootPath = AppContext.BaseDirectory,
    ApplicationName = "DomeMQ.Broker"
});

var exeDirectory = ResolveContentDirectory();
AddJsonIfPresent(builder.Configuration, Path.Combine(exeDirectory, "appsettings.json"));
AddJsonIfPresent(builder.Configuration, Path.Combine(exeDirectory, "domemq.json"));
AddJsonIfPresent(builder.Configuration, Path.Combine(AppContext.BaseDirectory, "appsettings.json"));
AddJsonIfPresent(builder.Configuration, Path.Combine(AppContext.BaseDirectory, "domemq.json"));
AddJsonIfPresent(builder.Configuration, "/config/appsettings.json");
AddJsonIfPresent(builder.Configuration, "/config/domemq.json");
if (!string.IsNullOrWhiteSpace(configPath))
{
    builder.Configuration.AddJsonFile(configPath, optional: false, reloadOnChange: true);
}

builder.Configuration.AddEnvironmentVariables();
if (commandArgs.Length > 0)
{
    builder.Configuration.AddCommandLine(commandArgs);
}

var options = new DomeMqOptions();
builder.Configuration.GetSection("DomeMQ").Bind(options, bind => bind.ErrorOnUnknownConfiguration = true);
var errors = DomeMqOptionsValidator.Validate(options);
if (errors.Count > 0)
{
    foreach (var error in errors)
    {
        Console.Error.WriteLine(error);
    }

    return 1;
}

if (!DomeMqOptionsValidator.TryParseLogLevel(options.Logging.MinimumLevel, out var brokerLevel))
{
    brokerLevel = BrokerLogLevel.Information;
}

var minimum = ToLogLevel(brokerLevel);
builder.Logging.SetMinimumLevel(minimum);
builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
if (options.Logging.Console)
{
    builder.Logging.AddSimpleConsole(console =>
    {
        console.SingleLine = true;
        console.TimestampFormat = "HH:mm:ss ";
    });
}

if (options.Logging.File)
{
    builder.Logging.AddProvider(new FileLoggerProvider(options.Logging.FilePath, minimum));
}

builder.Services.AddSingleton(options);
builder.Services.AddSingleton<IConfiguration>(builder.Configuration);
builder.Services.AddHostedService<BrokerService>();

await builder.Build().RunAsync().ConfigureAwait(false);
return 0;

static void AddJsonIfPresent(IConfigurationManager configuration, string path)
{
    if (File.Exists(path))
    {
        configuration.AddJsonFile(path, optional: true, reloadOnChange: true);
    }
}

static string ResolveContentDirectory()
{
    var process = Environment.ProcessPath;
    if (!string.IsNullOrEmpty(process))
    {
        var name = Path.GetFileName(process);
        if (!name.Equals("dotnet", StringComparison.OrdinalIgnoreCase)
            && !name.Equals("dotnet.exe", StringComparison.OrdinalIgnoreCase))
        {
            return Path.GetDirectoryName(process) ?? AppContext.BaseDirectory;
        }
    }

    return AppContext.BaseDirectory;
}

static string? TakeConfigPath(ref string[] args)
{
    string? path = Environment.GetEnvironmentVariable("DOMEMQ_CONFIG");
    var rest = new List<string>(args.Length);
    for (var i = 0; i < args.Length; i++)
    {
        if (args[i] == "--config" && i + 1 < args.Length)
        {
            path = args[++i];
            continue;
        }

        if (args[i].StartsWith("--config=", StringComparison.Ordinal))
        {
            path = args[i]["--config=".Length..];
            continue;
        }

        rest.Add(args[i]);
    }

    args = rest.ToArray();
    return path;
}

static LogLevel ToLogLevel(BrokerLogLevel level) => level switch
{
    BrokerLogLevel.Trace => LogLevel.Trace,
    BrokerLogLevel.Debug => LogLevel.Debug,
    BrokerLogLevel.Information => LogLevel.Information,
    BrokerLogLevel.Warning => LogLevel.Warning,
    BrokerLogLevel.Error => LogLevel.Error,
    BrokerLogLevel.Critical => LogLevel.Critical,
    _ => LogLevel.None
};

internal sealed class BrokerService(DomeMqOptions options, IConfiguration configuration, ILogger<BrokerService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var broker = new DomeMqBroker(options, new BrokerLoggerAdapter(logger));
        await broker.StartAsync(stoppingToken).ConfigureAwait(false);
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                try
                {
                    if (configuration is IConfigurationRoot root)
                    {
                        root.Reload();
                    }

                    var reloaded = new DomeMqOptions();
                    configuration.GetSection("DomeMQ").Bind(reloaded);
                    broker.ReloadHotConfiguration(reloaded);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Hot reload failed. The previous IP access rules and TLS keys stay in effect.");
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            await broker.StopAsync(CancellationToken.None).ConfigureAwait(false);
            await broker.DisposeAsync().ConfigureAwait(false);
        }
    }
}
