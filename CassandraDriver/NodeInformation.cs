namespace CassandraDriver;

/// <summary>
/// A class used to store variouhs information about a node
/// </summary>
public class NodeInformation
{
    /// <summary>
    /// The primary constructor for creating the class
    /// </summary>
    /// <param name="ipAddress">The ip address for the node</param>
    /// <param name="port">The port for the node, defaults to 9042</param>
    public NodeInformation(string ipAddress, int port = 9042)
    {
        this.IpAddress = ipAddress;
        this.Port = port;
    }

    /// <summary>
    /// The nodes IP address
    /// </summary>
    public string IpAddress { get; init; }

    /// <summary>
    /// The nodes port
    /// </summary>
    public int Port { get; init; }
}
