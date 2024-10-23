using System.Collections.Generic;
using CassandraDriver.Results;

namespace CassandraDriver;

/// <summary>
/// A class that represents a statement that will be passed to a query operation
/// </summary>
public abstract class Statement
{
    /// <summary>
    /// Creates a new statement
    /// </summary>
    /// <param name="query">The query to be used</param>
    /// <param name="pagingState">The paging state if there is any</param>
    /// <param name="itemsPerPage">The items per page limit</param>
    /// <param name="columns">Pre cached columns if there are any</param>
    /// <param name="parameters">Parameters to bind</param>
    protected Statement(
        byte[]? pagingState = null,
        int itemsPerPage = 5000,
        IReadOnlyList<Column>? columns = null,
        object?[]? parameters = null)
    {
        this.PagingState = pagingState;
        this.ItemsPerPage = itemsPerPage;
        this.Columns = columns;
        this.Parameters = parameters;
    }

    /// <summary>
    /// The paging state for the statement
    /// </summary>
    public byte[]? PagingState { get; internal set; }

    /// <summary>
    /// How many items per page
    /// Defaults to 5000
    /// </summary>
    public int ItemsPerPage { get; }

    /// <summary>
    /// Pre cached columns
    /// Useful if columns are already known and no metadata from the database is needed
    /// </summary>
    public IReadOnlyList<Column>? Columns { get; }

    /// <summary>
    /// Parameters to build with
    /// </summary>
    public object?[]? Parameters { get; } = null;

    /// <summary>
    /// Creates a <see cref="StatementBuilder"/> with the provided query
    /// </summary>
    /// <param name="query"></param>
    /// <returns></returns>
    public static StatementBuilder WithQuery(string query) => new(query);

    /// <summary>
    /// Creates a <see cref="StatementBuilder"/> with the providede prepared ID
    /// </summary>
    /// <param name="preparedId"></param>
    /// <returns></returns>
    public static StatementBuilder WithPreparedId(byte[] preparedId) => new(preparedId);

    /// <summary>
    /// Converts strings to statements
    /// </summary>
    /// <param name="query"></param>
    /// <returns></returns>
    public static implicit operator Statement(string query) => WithQuery(query).Build();
}
