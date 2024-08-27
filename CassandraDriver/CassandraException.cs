using System;

namespace CassandraDriver;

public sealed class CassandraException : Exception
{
    internal CassandraException(string message, bool isServer = false) : base(isServer
        ? $"Got error from server: {message}"
        : message)
    {
    }
}
