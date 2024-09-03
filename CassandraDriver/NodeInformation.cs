namespace CassandraDriver;

public class NodeInformation
{
    public NodeInformation(string ipAddress, int port = 9042)
    {
        this.IpAddress = ipAddress;
        this.Port = port;
    }

    public string IpAddress { get; init; }
    public int Port { get; init; }
}
