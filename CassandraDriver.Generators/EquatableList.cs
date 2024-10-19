using System;
using System.Collections.Generic;
using System.Linq;

namespace CassandraDriver.Generators;

internal class EquatableList<T> : List<T>, IEquatable<EquatableList<T>>
{
    public bool Equals(EquatableList<T>? other)
    {
        if (other is null || this.Count != other.Count)
        {
            return false;
        }

        for (int i = 0; i < this.Count; i++)
        {
            if (!EqualityComparer<T>.Default.Equals(this[i], other[i]))
            {
                return false;
            }
        }

        return true;
    }

    public override bool Equals(object? obj) => Equals(obj as EquatableList<T>);

    public override int GetHashCode() =>
        this.Select(item => item?.GetHashCode() ?? 0).Aggregate((x, y) => x ^ y);
}
