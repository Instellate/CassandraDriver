using System.Net;
using CassandraDriver.Results;

namespace CassandraDriver.Tests;

public class ClusterTest
{
    private CassandraCluster _cluster;

    [OneTimeSetUp]
    public async Task SetupAsync()
    {
        CassandraClusterBuilder builder = CassandraClusterBuilder
            .CreateBuilder()
            .AddNode("172.42.0.2")
            .DiscoverOtherNodes()
            .BlockKeyspace("system")
            .BlockKeyspace("system_auth")
            .SetDefaultKeyspace("csharpdriver");
        this._cluster = await builder.BuildAsync();
    }

    [OneTimeTearDown]
    public async Task TearDownAsync()
    {
        await this._cluster.DisconnectAsync();
    }

    [Test]
    public async Task BuildClusterAsync()
    {
        Assert.That(this._cluster.NodeCount, Is.EqualTo(3));

        Statement state = Statement
            .WithQuery("SELECT * FROM person WHERE name = ?")
            .WithParameters("Instellate")
            .Build();

        Query query = await this._cluster.QueryAsync(state);
        Assert.That(query.Count, Is.EqualTo(1));

        Assert.IsInstanceOf<string>(query[0]["name"]);
        Assert.IsInstanceOf<int>(query[0]["user_id"]);
        Assert.IsInstanceOf<DateTime>(query[0]["created_at"]);
        Assert.IsInstanceOf<IPAddress>(query[0]["ip_addr"]);

        Assert.That((string)query[0]["name"]!, Is.EqualTo("Instellate"));
        Assert.That((int)query[0]["user_id"]!, Is.EqualTo(0));
        Assert.That(
            (DateTime)query[0]["created_at"]!,
            Is.EqualTo(DateTime.Parse("2023-07-21"))
        );
        Assert.That(
            (IPAddress)query[0]["ip_addr"]!,
            Is.EqualTo(IPAddress.Parse("178.163.120.7"))
        );
    }
}
