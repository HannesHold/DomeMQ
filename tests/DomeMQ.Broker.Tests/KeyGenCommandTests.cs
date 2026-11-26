using DomeMQ.Broker;

namespace DomeMQ.Broker.Tests;

public class KeyGenCommandTests
{
    [Fact]
    public void Command_writes_pem_and_reports_the_paths()
    {
        var output = Path.Combine(Path.GetTempPath(), "domemq-cli-" + Guid.NewGuid().ToString("N"));
        var stdout = new StringWriter();
        var stderr = new StringWriter();
        try
        {
            var code = KeyGenCommand.Run(["--out", output, "--server", "alpha.dome,beta.dome", "--client", "worker", "--format", "pem"], stdout, stderr);
            Assert.Equal(0, code);
            Assert.Empty(stderr.ToString());
            Assert.True(File.Exists(Path.Combine(output, "ca.crt")));
            Assert.True(File.Exists(Path.Combine(output, "servers", "beta.dome.key")));
            Assert.True(File.Exists(Path.Combine(output, "servers", "beta.dome.spki.sha256")));
            Assert.True(File.Exists(Path.Combine(output, "clients", "worker.crt")));
            Assert.Contains(output, stdout.ToString());
        }
        finally
        {
            if (Directory.Exists(output))
            {
                Directory.Delete(output, recursive: true);
            }
        }
    }

    [Fact]
    public void Missing_server_name_is_an_error()
    {
        var stderr = new StringWriter();
        var code = KeyGenCommand.Run(["--out", "keys"], TextWriter.Null, stderr);
        Assert.Equal(1, code);
        Assert.Contains("Usage", stderr.ToString());
    }
}
