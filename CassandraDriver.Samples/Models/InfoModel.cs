using CassandraDriver.Serialization;

namespace CassandraDriver.Samples.Models;

[CqlDeserialize]
public partial class InfoModel
{
    [CqlColumnName("real_name")]
    public required string RealName { get; set; }
}
