using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using CassandraDriver.Frames;
using CassandraDriver.Results;
using IntervalTree;

namespace CassandraDriver;

public class CassandraPool
{
    private readonly
        ConcurrentDictionary<KeyspaceTableHash, IntervalTree<long, CassandraClient>>
        _intervals;

    private readonly IReadOnlyList<CassandraClient> _nodes;
    private readonly ConcurrentDictionary<string, PoolPrepared> _prepareds = [];

    public int NodeCount => this._nodes.Count;

    internal CassandraPool(
        ConcurrentDictionary<KeyspaceTableHash, IntervalTree<long, CassandraClient>>
            intervals, IReadOnlyList<CassandraClient> nodes)
    {
        this._intervals = intervals;
        this._nodes = nodes;
    }

    public async Task<Query> QueryAsync(string query, params object[] param)
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
            if (param[i].GetType() != type)
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

        // It wouldn't matter if an exception is thrown if all nodes are dead
        CassandraClient? node = null;
        if (findIndex > -1)
        {
            object value = param[findIndex];
            long murmur3Hash = CassandraMurmur3Hash.CalculatePrimaryKey(
                CqlValue.CreateCqlValue(value).Bytes
            );

            KeyspaceTableHash hash = new(
                prepared.Columns[0].Keyspace!, // TODO: Not this
                prepared.Columns[0].Table!
            );

            IntervalTree<long, CassandraClient> interval
                = this._intervals.GetValueOrDefault(hash)!;

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

        return await node.ExecuteAsync(id, param);
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
}
