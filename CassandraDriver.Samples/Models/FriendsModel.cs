using CassandraDriver.Serialization;

namespace CassandraDriver.Samples.Models;

[CqlDeserialize]
public partial class FriendsModel
{
    [CqlColumnName("friend_name")]
    public required string FriendName { get; set; }

    [CqlColumnName("friend_since")]
    public required DateTime FriendsSince { get; init; }
}
