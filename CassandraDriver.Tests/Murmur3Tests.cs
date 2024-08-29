using System.Buffers.Binary;
using System.Diagnostics;
using System.Text;

namespace CassandraDriver.Tests;

public class Murmur3Tests
{
    private string[] _strings;
    private CassandraClient _client;

    [SetUp]
    public async Task SetUpAsync()
    {
        this._client = new CassandraClient("localhost", defaultKeyspace: "csharpdriver");
        await this._client.ConnectAsync();
    }

    [SetUp]
    public void SetUp()
    {
        this._strings = File.ReadAllLines("./vectors.txt");
    }

    [Test]
    public void TestAgainstVectors()
    {
        byte[] bytes = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, 31);

        Stopwatch sw = new();
        sw.Start();
        foreach (string line in this._strings)
        {
            string[] tabs = line.Split('\t');
            string value = tabs[0];
            long hash1 = long.Parse(tabs[1]);
            long hash2 = long.Parse(tabs[2]);

            (long, long) hashes =
                CassandraMurmur3Hash.CalculateHash(Encoding.UTF8.GetBytes(value), 0);
            Assert.That(hashes.Item1, Is.EqualTo(hash1));
            Assert.That(hashes.Item2, Is.EqualTo(hash2));
        }

        sw.Stop();
        Console.WriteLine(
            $"Took {sw.ElapsedMilliseconds}ms to hash 1000 values and compare"
        );
    }

    [Test]
    public async Task TestAgainstDatabaseAsync()
    {
        Query query =
            await this._client.QueryAsync(
                "SELECT token(name) AS name_token, name FROM person"
            );

        foreach (Row row in query.Rows)
        {
            Assert.IsInstanceOf<long>(row["name_token"]);
            Assert.IsInstanceOf<string>(row["name"]);

            long token = (long)row["name_token"];
            string name = (string)row["name"];

            long hash =
                CassandraMurmur3Hash.CalculatePrimaryKey(Encoding.UTF8.GetBytes(name));
            Assert.That(token, Is.EqualTo(hash));
        }
    }
}
