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
            "SELECT name, user_id, created_at, ip_addr FROM person WHERE name = ?",
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
            await this._client.QueryAsync(
                "SELECT name, user_id, created_at, ip_addr FROM person WHERE user_id = ?",
                1);
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
            await this._client.PrepareAsync(
                "SELECT name, user_id, created_at, ip_addr FROM person WHERE name = ?");
        Assert.That(prepared.BindMarkers.Count, Is.EqualTo(1));
        Assert.That(prepared.Columns.Count, Is.EqualTo(4));

        Assert.That(prepared.BindMarkers[0].Name, Is.EqualTo("name"));
        Assert.That(prepared.BindMarkers[0].PartitionKeyIndex, Is.EqualTo(0));

        await this._client.ExecuteAsync(prepared.Id, "Instellate");
    }

    [Test]
    public async Task TestUdtAsync()
    {
        Query query
            = await this._client.QueryAsync("SELECT friends FROM person WHERE name = ?",
                "Instellate");

        Assert.IsInstanceOf<List<Row>>(query[0]["friends"]);

        List<Row> udt = (List<Row>)query[0]["friends"]!;

        Assert.IsInstanceOf<int>(udt[0]["friend_id"]);
        Assert.IsInstanceOf<DateTimeOffset>(udt[0]["friends_since"]);

        Assert.That(udt[0]["friend_id"], Is.EqualTo(1));
        Assert.That(udt[0]["friends_since"],
            Is.EqualTo(DateTimeOffset.Parse("2024-09-13 12:43:56+00:00")));
    }

    [Test]
    public async Task TestMapAsync()
    {
        Query query
            = await this._client.QueryAsync("SELECT houses FROM person WHERE name = ?",
                "Instellate");

        Row row = query[0];

        Assert.IsInstanceOf<Dictionary<string, string>>(row["houses"]);

        Dictionary<string, string> houses = (Dictionary<string, string>)row["houses"]!;
        Assert.That(houses["Riksgatan 1, 100 12 Stockholm"],
            Is.EqualTo("The Swedish Parliament House"));
    }

    [Test]
    public async Task TestQueryWithPagesAsync()
    {
        CassandraPager pager
            = await this._client.QueryWithPagesAsync("SELECT * FROM person", 1);

        int count = 0;
        await foreach (Row _ in pager)
        {
            count++;
        }

        Assert.That(count, Is.EqualTo(2));
    }

    [Test]
    public async Task TestExecuteWithPagesAsync()
    {
        Prepared prepared
            = await this._client.PrepareAsync("SELECT * FROM person");

        CassandraPager pager = await this._client.ExecuteWithPagesAsync(prepared.Id, 1);

        int count = 0;
        await foreach (Row _ in pager)
        {
            count++;
        }

        Assert.That(count, Is.EqualTo(2));
    }
}
