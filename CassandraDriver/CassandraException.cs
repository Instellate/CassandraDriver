using System;

namespace CassandraDriver;

/// <summary>
/// The general exception used by the library
/// </summary>
public sealed class CassandraException : Exception
{
    internal CassandraException(string message, bool isServer = false) : base(isServer
        ? $"Got error from server: {message}"
        : message)
    {
    }
}
