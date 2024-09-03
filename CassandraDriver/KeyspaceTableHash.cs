using System;

namespace CassandraDriver;

internal class KeyspaceTableHash
{
    public KeyspaceTableHash(string keyspace, string table)
    {
        this.Keyspace = keyspace;
        this.Table = table;
    }

    public string Keyspace { get; }
    public string Table { get; }

    public override int GetHashCode()
    {
        return HashCode.Combine(this.Keyspace.GetHashCode(), this.Table.GetHashCode());
    }

    public override bool Equals(object? obj)
    {
        if (obj is KeyspaceTableHash hash)
        {
            return hash.Keyspace == this.Keyspace && hash.Table == this.Table;
        }

        return Equals(this, obj);
    }
}
