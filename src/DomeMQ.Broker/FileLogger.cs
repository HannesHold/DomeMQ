using DomeMQ.Server;
using Microsoft.Extensions.Logging;

namespace DomeMQ.Broker;

internal sealed class FileLoggerProvider : ILoggerProvider
{
    private readonly LogLevel _minimum;
    private readonly object _gate = new();
    private readonly StreamWriter _writer;

    public FileLoggerProvider(string path, LogLevel minimum)
    {
        _minimum = minimum;
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        _writer = new StreamWriter(new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read))
        {
            AutoFlush = true
        };
    }

    public ILogger CreateLogger(string categoryName) => new FileLogger(categoryName, _minimum, _writer, _gate);

    public void Dispose() => _writer.Dispose();

    private sealed class FileLogger(string category, LogLevel minimum, StreamWriter writer, object gate) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= minimum && logLevel != LogLevel.None;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            var line = $"{DateTimeOffset.Now:O} {logLevel} {category}: {formatter(state, exception)}";
            if (exception is not null)
            {
                line += Environment.NewLine + exception;
            }

            lock (gate)
            {
                writer.WriteLine(line);
            }
        }
    }
}

internal sealed class BrokerLoggerAdapter(ILogger logger) : IBrokerLogger
{
    public void Log(BrokerLogLevel level, string category, string message, Exception? exception = null)
    {
        var mapped = level switch
        {
            BrokerLogLevel.Trace => LogLevel.Trace,
            BrokerLogLevel.Debug => LogLevel.Debug,
            BrokerLogLevel.Information => LogLevel.Information,
            BrokerLogLevel.Warning => LogLevel.Warning,
            BrokerLogLevel.Error => LogLevel.Error,
            BrokerLogLevel.Critical => LogLevel.Critical,
            _ => LogLevel.None
        };
        logger.Log(mapped, exception, "[{Category}] {Message}", category, message);
    }
}
