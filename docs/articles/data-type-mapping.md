---
uid: articles.data-type-mapping
title: Data type mapping
---

# Data type mapping

A table of how the CQL values get's mapped to .NET values.

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