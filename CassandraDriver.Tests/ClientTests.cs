using System.Diagnostics;

namespace CassandraDriver.Tests;

public class ClientTests
{
    private CassandraClient _client;

    [OneTimeSetUp]
    public async Task SetupAsync()
    {
        this._client = new CassandraClient("172.17.0.2", defaultKeyspace: "csharpdriver");
        await this._client.ConnectAsync();
        Trace.Listeners.Add(new ConsoleTraceListener());
        TaskScheduler.UnobservedTaskException += HandleUnhandledTaskExceptions;
    }

    public static void HandleUnhandledTaskExceptions(
        object? sender,
        UnobservedTaskExceptionEventArgs e
    )
    {
        Console.WriteLine(e);
    }

    [OneTimeTearDown]
    public async Task EndTestAsync()
    {
        await this._client.DisconnectAsync();
        this._client.Dispose();
        Trace.Flush();
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

        Query errorQuery =
            await this._client.QueryAsync("SELECT * FROM person WHERE user_id = ?", 1);
        Assert.IsInstanceOf<string>(errorQuery[0]["name"]);
        Assert.IsInstanceOf<int>(errorQuery[0]["user_id"]);

        StringAssert.Contains("Flaze", (string)errorQuery[0]["name"]);
        Assert.That((int)errorQuery[0]["user_id"], Is.EqualTo(1));

        Assert.IsNotNull(errorQuery.Warnings);
        StringAssert.AreEqualIgnoringCase(
            "This query should use ALLOW FILTERING and will be rejected in future versions.",
            errorQuery.Warnings[0]
        );
    }
}
