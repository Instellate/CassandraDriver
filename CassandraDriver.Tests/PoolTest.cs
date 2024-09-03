using System.Net;
using CassandraDriver.Results;

namespace CassandraDriver.Tests;

public class PoolTest
{
    private CassandraPool _pool;

    [OneTimeSetUp]
    public async Task SetupAsync()
    {
        CassandraPoolBuilder builder = CassandraPoolBuilder
            .CreateBuilder()
            .AddNode("172.42.0.2")
            .DiscoverOtherNodes()
            .BlockKeyspace("system")
            .SetDefaultKeyspace("csharpdriver");
        _pool = await builder.BuildAsync();
    }

    [Test]
    public async Task BuildPoolAsync()
    {
        Assert.That(_pool.NodeCount, Is.EqualTo(3));

        Query query =
            await _pool.QueryAsync("SELECT * FROM person WHERE name = ?", "Instellate");
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
