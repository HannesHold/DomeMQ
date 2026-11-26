namespace DomeMQ.Inspector.Services;

public enum InspectorMode
{
    Choose,
    Server,
    Client
}

public static class LaunchOptions
{
    public static InspectorMode Mode { get; private set; } = InspectorMode.Choose;

    public static string? ConfigPath { get; private set; }

    public static string? ScreenshotDirectory { get; private set; }

    public static void Parse(string[] args)
    {
        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (arg is "--mode" or "-m")
            {
                var value = Next(args, ref i);
                Mode = value.ToLowerInvariant() switch
                {
                    "server" => InspectorMode.Server,
                    "client" => InspectorMode.Client,
                    _ => throw new ArgumentException("Expected --mode server or --mode client.")
                };
            }
            else if (arg is "--config" or "-c")
            {
                ConfigPath = Next(args, ref i);
            }
            else if (arg == "--screenshot")
            {
                ScreenshotDirectory = Next(args, ref i);
            }
        }
    }

    private static string Next(string[] args, ref int index)
    {
        if (index + 1 >= args.Length)
        {
            throw new ArgumentException($"Missing value after {args[index]}.");
        }

        index++;
        return args[index];
    }
}
