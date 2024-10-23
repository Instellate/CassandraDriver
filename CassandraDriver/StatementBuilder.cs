using System.Collections.Generic;
using CassandraDriver.Results;

namespace CassandraDriver;

public sealed class StatementBuilder
{
    public StatementBuilder(string query)
    {
        this.Query = query;
    }

    public StatementBuilder(byte[] preparedId)
    {
        this.PreparedId = preparedId;
    }

    /// <summary>
    /// The query that will be used
    /// </summary>
    public string? Query { get; set; }

    /// <summary>
    /// The prepared id if it is suppose to be a execute statement.
    /// </summary>
    public byte[]? PreparedId { get; set; }

    /// <summary>
    /// The paging state for pagination
    /// </summary>
    public byte[]? PagingState { get; set; }

    /// <summary>
    /// How many items to get on a page. Defaults to 5000
    /// </summary>
    public int ItemsPerPage { get; set; } = 5000;

    /// <summary>
    /// Pre cached columns
    /// If provided the database will not send any columns and these will be used
    /// </summary>
    public IReadOnlyList<Column>? Columns { get; set; }

    /// <summary>
    /// Parameters to build with
    /// </summary>
    public object?[]? Parameters { get; set; } = null;

    /// <summary>
    /// Builds a Statement from the values provided to the statement builder
    /// </summary>
    /// <returns>A newly created statement</returns>
    public Statement Build()
    {
        if (this.Query is not null)
        {
            return new QueryStatement(Query,
                PagingState,
                ItemsPerPage,
                Columns,
                Parameters);
        }
        else if (this.PreparedId is not null)
        {
            return new ExecuteStatement(PreparedId,
                PagingState,
                ItemsPerPage,
                Columns,
                Parameters);
        }
        else
        {
            throw new CassandraException("Prepared id and query string was not provided");
        }
    }

    /// <summary>
    /// Sets the <see cref="PagingState"/> property
    /// </summary>
    /// <param name="pagingState"></param>
    /// <returns></returns>
    public StatementBuilder WithPagingState(byte[]? pagingState)
    {
        this.PagingState = pagingState;
        return this;
    }

    /// <summary>
    /// Sets the <see cref="ItemsPerPage"/> property
    /// </summary>
    /// <param name="itemsPerPage"></param>
    /// <returns></returns>
    public StatementBuilder WithItemsPerPage(int itemsPerPage)
    {
        this.ItemsPerPage = itemsPerPage;
        return this;
    }

    /// <summary>
    /// Sets the <see cref="Columns"/> property
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public StatementBuilder WithColumns(IReadOnlyList<Column>? columns)
    {
        this.Columns = columns;
        return this;
    }

    /// <summary>
    /// Sets the <see cref="Parameters"/> property
    /// </summary>
    /// <param name="parameters"></param>
    /// <returns></returns>
    public StatementBuilder WithParameters(params object?[]? parameters)
    {
        this.Parameters = parameters;
        return this;
    }
}
