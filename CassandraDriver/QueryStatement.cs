using System.Collections.Generic;
using CassandraDriver.Results;

namespace CassandraDriver;

/// <summary>
/// A class that represents a statement that will be passed to a query operation
/// </summary>
public sealed class QueryStatement : Statement
{
    /// <summary>
    /// Creates a new statement
    /// </summary>
    /// <param name="query">The query to be used</param>
    /// <param name="pagingState">The paging state if there is any</param>
    /// <param name="itemsPerPage">The items per page limit</param>
    /// <param name="columns">Pre cached columns if there are any</param>
    /// <param name="parameters">Parameters to bind</param>
    public QueryStatement(string query,
        byte[]? pagingState = null,
        int itemsPerPage = 5000,
        IReadOnlyList<Column>? columns = null,
        object?[]? parameters = null) : base(pagingState,
        itemsPerPage,
        columns,
        parameters)
    {
        this.Query = query;
    }

    /// <summary>
    /// The query that the statement uses
    /// </summary>
    public string Query { get; }

    /// <summary>
    /// Converts a string to <see cref="QueryStatement"/>
    /// </summary>
    /// <param name="str"></param>
    /// <returns></returns>
    public static implicit operator QueryStatement(string str) => new(str);
}
