using System.Net;
using CassandraDriver.Serialization;

namespace CassandraDriver.Samples.Models;

[CqlDeserialize]
public partial class UserModel
{
    [CqlColumnName("username")]
    public required string Username { get; init; }

    [CqlColumnName("password")]
    public required string Password { get; init; }

    [CqlColumnName("created_at")]
    public required DateTime CreatedAt { get; init; }

    [CqlColumnName("ip_address")]
    public required IPAddress IpAddress { get; init; }

    [CqlColumnName("info")]
    public required InfoModel Info { get; init; }

    [CqlColumnName("friends")]
    public required List<FriendsModel> Friends { get; init; }
}
