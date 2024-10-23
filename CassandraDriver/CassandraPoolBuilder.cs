using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using CassandraDriver.Results;
using IntervalTree;

namespace CassandraDriver;

/// <summary>
/// A builder used for building a cassandra pool
/// </summary>
public class CassandraPoolBuilder
{
    private readonly List<NodeInformation> _nodes = [];
    private readonly HashSet<string> _intervalBlockedKeyspaces = [];
    private bool _discoverNodes;
    private string? _defaultKeyspace;
    private int _defaultPort = 9042;

    /// <summary>
    /// Creates a builder
    /// </summary>
    /// <returns>The builder</returns>
    public static CassandraPoolBuilder CreateBuilder()
    {
        return new CassandraPoolBuilder();
    }

    /// <summary>
    /// Adds a new node
    /// </summary>
    /// <remarks>Usually you only need to add one node and activate <see cref="DiscoverOtherNodes"/> and rest of the nodes will be discovered automatically</remarks>
    /// <param name="information">The information for the node</param>
    /// <returns>The builder</returns>
    public CassandraPoolBuilder AddNode(NodeInformation information)
    {
        this._nodes.Add(information);
        return this;
    }

    /// <inheritdoc cref="AddNode(CassandraDriver.NodeInformation)"/>
    /// <param name="hostname">The hostname for the node</param>
    /// <param name="port">The port for the node</param>
    /// <returns>The builder</returns>
    public CassandraPoolBuilder AddNode(string hostname, int port = 9042)
    {
        AddNode(new NodeInformation(hostname, port));
        return this;
    }

    /// <summary>
    /// Allow the builder to discover rest of the nodes
    /// Requires at least one node
    /// </summary>
    /// <returns>The builder</returns>
    public CassandraPoolBuilder DiscoverOtherNodes()
    {
        this._discoverNodes = true;
        return this;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="keyspace"></param>
    /// <returns>The builder</returns>
    public CassandraPoolBuilder SetDefaultKeyspace(string keyspace)
    {
        this._defaultKeyspace = keyspace;
        return this;
    }

    /// <summary>
    /// Set the default port that is used when discovering and creating nodes
    /// </summary>
    /// <param name="port">The port to be used by default</param>
    /// <returns>The builder</returns>
    public CassandraPoolBuilder SetDefaultPort(int port)
    {
        this._defaultPort = port;
        return this;
    }

    /// <summary>
    /// Blocks a keyspace from being registered into the interval
    /// </summary>
    /// <remarks>
    /// Useful if you do not want to register all the system keyspaces in the intervals.
    /// They can take up a bit of memory as there's many of them.
    /// Also useful if you know the application uses only some specific keyspaces.
    /// It is recommended to block keyspace `system`.
    /// </remarks>
    /// <returns>The builder</returns>
    public CassandraPoolBuilder BlockKeyspace(string keyspace)
    {
        this._intervalBlockedKeyspaces.Add(keyspace);
        return this;
    }

    /// <summary>
    /// Builds the cassandra pool
    /// </summary>
    /// <returns>A newly created cassandra pool</returns>
    /// <exception cref="ArgumentOutOfRangeException">There was no node added</exception>
    public async Task<CassandraPool> BuildAsync()
    {
        if (this._nodes.Count <= 0)
        {
            throw new ArgumentOutOfRangeException(
                // ReSharper disable once NotResolvedInText
                "Nodes",
                "Cannot build without any nodes."
            );
        }

        Dictionary<string, CassandraClient> constructedNodes = new(this._nodes.Count);
        foreach (NodeInformation information in this._nodes)
        {
            CassandraClient node = new(
                information.IpAddress,
                information.Port,
                this._defaultKeyspace
            );
            await node.ConnectAsync();
            constructedNodes.Add(information.IpAddress, node);
        }

        if (this._discoverNodes)
        {
            await FindAllNodesAsync(constructedNodes);
        }

        CassandraClient helpNode = constructedNodes.First().Value;
        Dictionary<KeyspaceTableHash, IntervalTree<long, CassandraClient>> intervals
            = new();

        BaseStatement tokenRingStatement = BaseStatement
            .WithQuery("SELECT * FROM system.token_ring")
            .Build();

        Query tokenRanges = await helpNode.QueryAsync(tokenRingStatement);
        await foreach (Row row in tokenRanges)
        {
            string keyspace = (string)row["keyspace_name"]!;
            if (this._intervalBlockedKeyspaces.Contains(keyspace))
            {
                continue;
            }

            string table = (string)row["table_name"]!;
            long start = long.Parse((string)row["start_token"]!);
            long end = long.Parse((string)row["end_token"]!);
            string endpoint = ((IPAddress)row["endpoint"]!).ToString();
            KeyspaceTableHash hash = new(keyspace, table);

            if (intervals.TryGetValue(
                    hash,
                    out IntervalTree<long, CassandraClient>? interval
                ))
            {
                if (start > end)
                {
                    interval.Add(end, start, constructedNodes[endpoint]);
                }
                else
                {
                    interval.Add(start, end, constructedNodes[endpoint]);
                }
            }
            else
            {
                if (start > end)
                {
                    interval = new IntervalTree<long, CassandraClient>
                    {
                        { end, start, constructedNodes[endpoint] }
                    };
                    intervals.Add(hash, interval);
                }
                else
                {
                    interval = new IntervalTree<long, CassandraClient>
                    {
                        { start, end, constructedNodes[endpoint] }
                    };
                    intervals.Add(hash, interval);
                }
            }
        }

        return new CassandraPool(
            new ConcurrentDictionary<
                KeyspaceTableHash,
                IntervalTree<long, CassandraClient>
            >(intervals),
            constructedNodes.Values.ToList()
        );
    }

    private async ValueTask FindAllNodesAsync(Dictionary<string, CassandraClient> nodes)
    {
        CassandraClient client = nodes.Values.First();
        Query query = await client.QueryAsync("SELECT rpc_address FROM system.peers");
        nodes.EnsureCapacity(query.Count - nodes.Count);

        await foreach (Row row in query)
        {
            IPAddress rpcAddress = (IPAddress)row["rpc_address"]!;
            if (nodes.ContainsKey(rpcAddress.ToString()))
            {
                continue;
            }

            CassandraClient node = new(
                rpcAddress.ToString(),
                this._defaultPort,
                this._defaultKeyspace
            );
            await node.ConnectAsync();
            nodes.Add(rpcAddress.ToString(), node);
        }
    }
}
