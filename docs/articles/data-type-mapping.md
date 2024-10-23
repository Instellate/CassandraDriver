---
uid: articles.data-type-mapping
title: Data type mapping
---

# Data type mapping

A table of how the CQL values get's mapped to .NET values.  
All values listed here are the ones that is listed by [ScyllaDb](https://opensource.docs.scylladb.com/stable/cql/types.html)

| CQL Type     | .NET Type      |
|--------------|----------------|
| ascii        | string         |
| bigint       | long           |
| blob         | byte[]         |
| boolean      | bool           |
| counter      | int            |
| date         | DateTime*      |
| decimal      | Decimal*       |
| double       | double         |
| duration     | TimeSpan       |
| float        | float          |
| inet         | IpAdress       |
| int          | int            |
| smallint     | short          |
| text/varchar | string         |
| time         | TimeSpan       |
| timestamp    | DateTimeOffset |
| timeuuid     | Guid           |
| tinyint      | sbyte          |
| uuid         | Guid           |
| varint       | BigInteger     |

<small>*These types have a chance of overflowing</small>

`list<T>` will be given as `List<T>`.   
`set<T>` will be given as `HashSet<T>`.
`map<TKey, TValue>` will be given as `Dictionary<TKey, TValue>`.

### Notes
There is some issues with `list`, `set`, and `map`.   
With how they are implemented internally right now makes it not possible to deserialize lists in lists, maps in maps, sets in sets.  
If you are using those this driver will not support those