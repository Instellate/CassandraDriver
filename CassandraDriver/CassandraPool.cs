using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using CassandraDriver.Frames;
using CassandraDriver.Results;
using IntervalTree;

namespace CassandraDriver;

/// <summary>
/// Represent a pool of nodes, stores prepared statements for each node and also calculates which node a statement should go to
/// </summary>
public class CassandraPool : IDisposable
{
    private readonly
        ConcurrentDictionary<KeyspaceTableHash, IntervalTree<long, CassandraClient>>
        _intervals;

    private readonly IReadOnlyList<CassandraClient> _nodes;
    private readonly ConcurrentDictionary<string, PoolPrepared> _prepareds = [];

    /// <summary>
    /// The amount of nodes that was found
    /// </summary>
    public int NodeCount => this._nodes.Count;

    internal CassandraPool(
        ConcurrentDictionary<KeyspaceTableHash, IntervalTree<long, CassandraClient>>
            intervals,
        IReadOnlyList<CassandraClient> nodes)
    {
        this._intervals = intervals;
        this._nodes = nodes;
    }

    /// <summary>
    /// Query the pool. Querying will automatically prepare all statements if they don't exist, and execute it. It will also find the right place to query on and handles dead nodes.
    /// </summary>
    /// <param name="query">The query used when querying</param>
    /// <param name="param">Parameters provided</param>
    /// <returns>The result from the database</returns>
    /// <exception cref="CassandraException"></exception>
    public async Task<Query> QueryAsync(string query, params object?[] param)
    {
        if (!this._prepareds.TryGetValue(query, out PoolPrepared? prepared))
        {
            CassandraClient? aliveNode = FindAnyAliveNode();
            if (aliveNode is null)
            {
                throw new CassandraException("Could not find an alive node.");
            }

            prepared = new PoolPrepared(await aliveNode.PrepareAsync(query));
            prepared.NodeIds.TryAdd(aliveNode.Host, prepared.Id);
            this._prepareds.TryAdd(query, prepared);
        }

        ArgumentOutOfRangeException.ThrowIfNotEqual(
            prepared.BindMarkers.Count,
            param.Length,
            "param"
        );

        for (int i = 0; i < prepared.BindMarkers.Count; i++)
        {
            BindMarker bindMarker = prepared.BindMarkers[i];
            if (bindMarker.Type == ColumnValueType.Custom)
            {
                continue;
            }

            Type type = Row.DataTypeTypes[bindMarker.Type];
            if (param[i]?.GetType() != type)
            {
                throw new CassandraException(
                    $"Parameter {i} does not match expected type {type}"
                );
            }
        }

        int findIndex = -1;
        for (int i = 0; i < prepared.BindMarkers.Count; ++i)
        {
            if (prepared.BindMarkers[i].PartitionKeyIndex == 0)
            {
                findIndex = i;
            }
        }

        CassandraClient? node = null;
        if (findIndex > -1)
        {
            object? value = param[findIndex];
            long murmur3Hash = CassandraMurmur3Hash.CalculatePrimaryKey(
                CqlValue.CreateCqlValue(value).Bytes
            );

            KeyspaceTableHash hash = new(
                prepared.Columns[0].Keyspace!, // TODO: Not this
                prepared.Columns[0].Table!
            );

            IntervalTree<long, CassandraClient> interval = GetIntervalTree(hash);

            IEnumerable<CassandraClient> possibleNodes = interval.Query(murmur3Hash);
            foreach (CassandraClient possibleNode in possibleNodes)
            {
                if (!possibleNode.IsDead)
                {
                    node = possibleNode;
                    break;
                }
            }

            node ??= FindAnyAliveNode();
        }
        else
        {
            node = FindAnyAliveNode();
        }

        if (node is null)
        {
            throw new CassandraException("Could not find an alive node.");
        }

        if (!prepared.NodeIds.TryGetValue(node.Host, out byte[]? id))
        {
            Prepared newPrepared = await node.PrepareAsync(query);
            prepared.NodeIds.TryAdd(node.Host, newPrepared.Id);
            id = newPrepared.Id;
        }

        BaseStatement statement = BaseStatement
            .WithPreparedId(id)
            .WithColumns(prepared.Columns)
            .WithParameters(param)
            .Build();

        return await node.ExecuteAsync(statement);
    }

    /// <summary>
    /// Query the database and converts it into a deserializable object. For more info <see cref="QueryAsync"/>
    /// </summary>
    /// <param name="query"></param>
    /// <param name="param"></param>
    /// <typeparam name="T">The deserializable object used</typeparam>
    /// <returns>A list of T</returns>
    public async Task<List<T>> QueryAsync<T>(string query, params object[] param)
        where T : ICqlDeserializable<T>
    {
        Query result = await this.QueryAsync(query);

        List<T> list = new(result.Count);
        foreach (Row row in result.LocalRows)
        {
            list.Add(T.DeserializeRow(row));
        }

        return list;
    }

    private CassandraClient? FindAnyAliveNode()
    {
        CassandraClient? node = null;
        foreach (CassandraClient possibleNode in this._nodes)
        {
            if (!possibleNode.IsDead)
            {
                node = possibleNode;
                break;
            }
        }

        return node;
    }

    private IntervalTree<long, CassandraClient> GetIntervalTree(KeyspaceTableHash hash)
    {
        if (this._intervals.TryGetValue(hash,
                out IntervalTree<long, CassandraClient>? interval))
        {
            return interval;
        }
        else
        {
            KeyspaceTableHash newHash = new(hash.Keyspace, "<ALL>");
            if (this._intervals.TryGetValue(newHash,
                    out IntervalTree<long, CassandraClient>? tree))
            {
                return tree;
            }
            else
            {
                throw new CassandraException("Couldn't find a interval");
            }
        }
    }

    /// <summary>
    /// Disconnects all nodes in the pool
    /// </summary>
    public async Task DisconnectAsync()
    {
        List<Task> disconnectTasks = new(this._nodes.Count);
        foreach (CassandraClient cassandraClient in this._nodes)
        {
            disconnectTasks.Add(cassandraClient.DisconnectAsync());
        }

        await Task.WhenAll(disconnectTasks);
    }

    /// <summary>
    /// Dispose the pool
    /// </summary>
    public void Dispose()
    {
        foreach (CassandraClient cassandraClient in this._nodes)
        {
            cassandraClient.Dispose();
        }
    }
}
