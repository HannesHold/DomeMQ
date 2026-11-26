using DomeMQ.Server;
using Microsoft.Extensions.Configuration;

namespace DomeMQ.Broker.Tests;

public class ConfigTests
{
    [Fact]
    public void Shipped_appsettings_binds_and_matches_the_documented_defaults()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
        var configuration = new ConfigurationBuilder().AddJsonFile(path).Build();
        var options = new DomeMqOptions();
        configuration.GetSection("DomeMQ").Bind(options, bind => bind.ErrorOnUnknownConfiguration = true);

        var errors = DomeMqOptionsValidator.Validate(options);
        Assert.Empty(errors);
        Assert.Equal(DomeMqDefaults.ServerName, options.ServerName);
        Assert.Equal(DomeMqDefaults.MaxClients, options.Limits.MaxClients);
        Assert.Equal(DomeMqDefaults.MaxChannelsPerClient, options.Limits.MaxChannelsPerClient);
        Assert.Equal(DomeMqDefaults.MaxChannels, options.Limits.MaxChannels);
        Assert.Equal(DomeMqDefaults.MaxPayloadSize, options.Limits.MaxPayloadSize);
        Assert.Equal(16 * 1024 * 1024, options.Limits.MaxPayloadSize);
        Assert.Equal(DomeMqDefaults.MaxMessagesPerChannel, options.Limits.MaxMessagesPerChannel);
        Assert.Equal(DomeMqDefaults.MaxMessagesPerChannel, options.Limits.MaxQueueLengthPerChannel);
        Assert.Null(options.Limits.MaxBytesPerChannel);
        Assert.Equal(DomeMqDefaults.MaxQueuedBytes, options.Limits.MaxQueuedBytes);
        Assert.Equal(DomeMqDefaults.MaxMessagesPerClient, options.Limits.MaxMessagesPerClient);
        Assert.Equal(DomeMqDefaults.WarningPercent, options.Limits.WarningPercent);
        Assert.Equal(DomeMqDefaults.FullBehavior, options.Limits.FullBehavior);
        Assert.Empty(options.Channels.Overrides);
        Assert.Equal(DomeMqDefaults.NoDelay, options.Tcp.NoDelay);
        Assert.Equal(DomeMqDefaults.SocketBufferSize, options.Tcp.ReceiveBufferSize);
        Assert.Equal(DomeMqDefaults.SocketBufferSize, options.Tcp.SendBufferSize);
        Assert.Equal(DomeMqDefaults.Backlog, options.Tcp.Backlog);
        Assert.Equal(DomeMqDefaults.KeepAliveTimeSeconds, options.Tcp.KeepAliveTimeSeconds);
        Assert.Equal(DomeMqDefaults.KeepAliveIntervalSeconds, options.Tcp.KeepAliveIntervalSeconds);
        Assert.Null(options.Tcp.LingerSeconds);
        Assert.Equal(DomeMqDefaults.HandshakeTimeout, options.Timeouts.HandshakeTimeout);
        Assert.Equal(DomeMqDefaults.IdleTimeout, options.Timeouts.IdleTimeout);
        Assert.Equal(DomeMqDefaults.ReadTimeout, options.Timeouts.ReadTimeout);
        Assert.Equal(DomeMqDefaults.WriteTimeout, options.Timeouts.WriteTimeout);
        Assert.Equal(DomeMqDefaults.HeartbeatInterval, options.Timeouts.HeartbeatInterval);
        Assert.Equal("None", options.Authentication.Mode);
        Assert.True(options.Unix.DeleteStaleSocket);
        Assert.Null(options.Unix.Permissions);
        Assert.Equal(DomeMqDefaults.DefaultChannelType, options.Channels.DefaultType);
        Assert.Null(options.Channels.DefaultMessageTtl);
        Assert.Equal(DomeMqDefaults.OrphanChannelTtl, options.Channels.OrphanChannelTtl);
        Assert.Equal(DomeMqDefaults.CleanupInterval, options.Channels.CleanupInterval);
        Assert.False(options.Health.Enabled);
        Assert.Equal(DomeMqDefaults.HealthPort, options.Health.Port);
        Assert.Equal(DomeMqDefaults.HealthPath, options.Health.Path);
        Assert.False(options.IpAccess.Enabled);
        Assert.Equal("Allowlist", options.IpAccess.Mode);
        Assert.True(options.IpAccess.AllowLoopback);
        Assert.Equal(["127.0.0.1", "::1/128"], options.IpAccess.Entries);
        Assert.Equal(2, options.Listeners.Count);
        Assert.True(options.Listeners[0].Enabled);
        Assert.Equal("Tcp", options.Listeners[0].Transport);
        Assert.Equal(DomeMqDefaults.Port, options.Listeners[0].Port);
        Assert.Null(options.Listeners[0].MaxPayloadSize);
        Assert.Null(options.Listeners[0].IpAccess);
        Assert.False(options.Listeners[1].Enabled);
        Assert.Equal("/tmp/domemq.sock", options.Listeners[1].Path);
        Assert.Empty(options.Keys);
        Assert.False(options.Tls.Enabled);
        Assert.Null(options.Tls.KeyId);
        Assert.False(options.Tls.RequireClientCertificate);
        Assert.Empty(options.Tls.ClientAuthorities);
        Assert.Equal(DomeMqDefaults.LogLevel, options.Logging.MinimumLevel);
    }
}
