using DomeMQ.Server;

namespace DomeMQ.Broker;

public static class KeyGenCommand
{
    public static int Run(string[] args) => Run(args, Console.Out, Console.Error);

    public static int Run(string[] args, TextWriter stdout, TextWriter stderr)
    {
        string? output = null;
        var servers = new List<string>();
        var clients = new List<string>();
        string? password = null;
        var format = KeyGenFormat.Pem;
        var days = 825;
        var bits = 2048;

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (arg is "--help" or "-h")
            {
                stdout.WriteLine(Usage);
                return 0;
            }

            if (arg is "--out" or "--server" or "--client" or "--password" or "--format" or "--days" or "--bits")
            {
                if (i + 1 >= args.Length)
                {
                    stderr.WriteLine($"Missing value after {arg}.");
                    return 1;
                }

                var value = args[++i];
                switch (arg)
                {
                    case "--out":
                        output = value;
                        break;
                    case "--server":
                        AddNames(servers, value);
                        break;
                    case "--client":
                        AddNames(clients, value);
                        break;
                    case "--password":
                        password = value;
                        break;
                    case "--format":
                        if (!TryFormat(value, out format))
                        {
                            stderr.WriteLine("Format must be pem, pfx, or both.");
                            return 1;
                        }

                        break;
                    case "--days":
                        if (!int.TryParse(value, out days))
                        {
                            stderr.WriteLine("Days must be a number.");
                            return 1;
                        }

                        break;
                    case "--bits":
                        if (!int.TryParse(value, out bits))
                        {
                            stderr.WriteLine("Bits must be a number.");
                            return 1;
                        }

                        break;
                }

                continue;
            }

            stderr.WriteLine($"Unknown argument '{arg}'.");
            stderr.WriteLine(Usage);
            return 1;
        }

        if (string.IsNullOrWhiteSpace(output) || servers.Count == 0)
        {
            stderr.WriteLine(Usage);
            return 1;
        }

        try
        {
            var result = KeyGenerator.Generate(new KeyGenRequest
            {
                OutputDirectory = output,
                ServerNames = servers,
                ClientNames = clients,
                Password = password,
                Format = format,
                Days = days,
                RsaBits = bits
            });
            stdout.WriteLine($"Wrote {result.OutputDirectory}");
            stdout.WriteLine($"CA certificate {result.CaCertificatePath}");
            stdout.WriteLine($"CA key {result.CaKeyPath}");
            foreach (var path in result.ServerCertificatePaths)
            {
                stdout.WriteLine($"Server certificate {path}");
            }

            foreach (var path in result.ClientCertificatePaths)
            {
                stdout.WriteLine($"Client certificate {path}");
            }

            return 0;
        }
        catch (Exception ex) when (ex is ArgumentException or IOException or UnauthorizedAccessException)
        {
            stderr.WriteLine(ex.Message);
            return 1;
        }
    }

    private static void AddNames(List<string> target, string value)
    {
        foreach (var part in value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            target.Add(part);
        }
    }

    private static bool TryFormat(string text, out KeyGenFormat format)
    {
        if (text.Equals("pem", StringComparison.OrdinalIgnoreCase))
        {
            format = KeyGenFormat.Pem;
            return true;
        }

        if (text.Equals("pfx", StringComparison.OrdinalIgnoreCase))
        {
            format = KeyGenFormat.Pfx;
            return true;
        }

        if (text.Equals("both", StringComparison.OrdinalIgnoreCase))
        {
            format = KeyGenFormat.Both;
            return true;
        }

        format = default;
        return false;
    }

    public const string Usage =
        """
        Usage: DomeMQ.Broker keygen --out <dir> --server <name>[,<name>] [--client <name>] [--password <secret>] [--format pem|pfx|both] [--days 825] [--bits 2048]

        Writes a CA, one server certificate per name (DNS SAN equals the name), and optional mTLS client certificates.
        PEM private keys are encrypted when --password is set. Server certificate files include the CA so the chain can be sent.
        Each server also gets a <name>.spki.sha256 file for client pinning.
        """;
}
