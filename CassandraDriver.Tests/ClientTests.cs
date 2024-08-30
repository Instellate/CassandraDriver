using System.Diagnostics;

namespace CassandraDriver.Tests;

public class ClientTests
{
    private CassandraClient _client;

    [OneTimeSetUp]
    public async Task SetupAsync()
    {
        this._client = new CassandraClient("localhost", defaultKeyspace: "csharpdriver");
        await this._client.ConnectAsync();
    }

    [Test]
    public void TestConnection()
    {
        Assert.True(this._client.Connected);
    }

    [Test]
    public async Task TestQueryingAsync()
    {
        Query query = await this._client.QueryAsync(
            "SELECT * FROM person WHERE name = ?",
            "Instellate"
        );

        Assert.IsInstanceOf<string>(query[0]["name"]);
        Assert.IsInstanceOf<int>(query[0]["user_id"]);

        StringAssert.Contains("Instellate", (string)query[0]["name"]);
        Assert.That((int)query[0]["user_id"], Is.EqualTo(0));

        Query warningQuery =
            await this._client.QueryAsync("SELECT * FROM person WHERE user_id = ?", 1);
        Assert.IsInstanceOf<string>(warningQuery[0]["name"]);
        Assert.IsInstanceOf<int>(warningQuery[0]["user_id"]);

        StringAssert.Contains("Flaze", (string)warningQuery[0]["name"]);
        Assert.That((int)warningQuery[0]["user_id"], Is.EqualTo(1));

        Assert.IsNotNull(warningQuery.Warnings);
        StringAssert.AreEqualIgnoringCase(
            "This query should use ALLOW FILTERING and will be rejected in future versions.",
            warningQuery.Warnings[0]
        );

        Query setQuery = await this._client.QueryAsync("SELECT tokens FROM system.local");
        Assert.IsInstanceOf<HashSet<string>>(setQuery[0]["tokens"]);
    }
}
