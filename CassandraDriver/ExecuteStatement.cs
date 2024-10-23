using System.Collections.Generic;
using CassandraDriver.Results;

namespace CassandraDriver;

/// <summary>
/// For execute statements
/// </summary>
public sealed class ExecuteStatement : BaseStatement
{
    /// <summary>
    /// The prepared ID to be used
    /// </summary>
    public byte[] PreparedId { get; }

    /// <summary>
    /// Creates a new statement
    /// </summary>
    /// <param name="preparedId">The prepared ID to be used</param>
    /// <param name="pagingState">The paging state if there is any</param>
    /// <param name="itemsPerPage">The items per page limit</param>
    /// <param name="columns">Pre cached columns if there are any</param>
    /// <param name="parameters">Parameters to bind</param>
    public ExecuteStatement(byte[] preparedId,
        byte[]? pagingState = null,
        int itemsPerPage = 5000,
        IReadOnlyList<Column>? columns = null,
        object?[]? parameters = null) : base(pagingState,
        itemsPerPage,
        columns,
        parameters)
    {
        this.PreparedId = preparedId;
    }
}
