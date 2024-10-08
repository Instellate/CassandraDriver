using System.Net;
using CassandraDriver.Results;

namespace CassandraDriver.Tests;

public class ClientTests
{
    private CassandraClient _client;

    [OneTimeSetUp]
    public async Task SetupAsync()
    {
        this._client = new CassandraClient("172.42.0.2", defaultKeyspace: "csharpdriver");
        await this._client.ConnectAsync();
    }

    [Test]
    public void TestConnection()
    {
        Assert.True(this._client.Connected);
    }

    [OneTimeTearDown]
    public async Task TearDownAsync()
    {
        await this._client.DisconnectAsync();
        this._client.Dispose();
    }

    [Test]
    public async Task TestQueryingAsync()
    {
        Query query = await this._client.QueryAsync(
            "SELECT * FROM person WHERE name = ?",
            "Instellate"
        );
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

        Query warningQuery =
            await this._client.QueryAsync("SELECT * FROM person WHERE user_id = ?", 1);
        Assert.That(query.Count, Is.EqualTo(1));

        Assert.IsInstanceOf<string>(warningQuery[0]["name"]);
        Assert.IsInstanceOf<int>(warningQuery[0]["user_id"]);

        Assert.That((string)warningQuery[0]["name"]!, Is.EqualTo("Flaze"));
        Assert.That((int)warningQuery[0]["user_id"]!, Is.EqualTo(1));
        Assert.That(
            (DateTime)warningQuery[0]["created_at"]!,
            Is.EqualTo(DateTime.Parse("2024-08-27"))
        );
        Assert.That(
            (IPAddress)warningQuery[0]["ip_addr"]!,
            Is.EqualTo(IPAddress.Parse("d060:f059:03e8:3bbf:7f7f:d83f:d782:26ba"))
        );


        Assert.IsNotNull(warningQuery.Warnings);
        Assert.That(
            warningQuery.Warnings[0],
            Is.EqualTo(
                "This query should use ALLOW FILTERING and will be rejected in future versions."
            )
        );

        Query setQuery = await this._client.QueryAsync("SELECT tokens FROM system.local");
        Assert.IsInstanceOf<HashSet<string>>(setQuery[0]["tokens"]);
    }

    [Test]
    public async Task TestPreparedAsync()
    {
        Prepared prepared =
            await this._client.PrepareAsync("SELECT * FROM person WHERE name = ?");
        Assert.That(prepared.BindMarkers.Count, Is.EqualTo(1));
        Assert.That(prepared.Columns.Count, Is.EqualTo(4));

        Assert.That(prepared.BindMarkers[0].Name, Is.EqualTo("name"));
        Assert.That(prepared.BindMarkers[0].PartitionKeyIndex, Is.EqualTo(0));

        await this._client.ExecuteAsync(prepared.Id, "Instellate");
    }
}
