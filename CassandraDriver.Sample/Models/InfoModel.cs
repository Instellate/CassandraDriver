using CassandraDriver.Serialization;

namespace CassandraDriver.Sample.Models;

[CqlDeserialize]
public partial class InfoModel
{
    [CqlColumnName("real_name")]
    public required string RealName { get; set; }
}
