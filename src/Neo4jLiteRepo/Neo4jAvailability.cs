using Neo4j.Driver;

namespace Neo4jLiteRepo;

public interface INeo4jAvailabilityReporter
{
    void ReportNeo4jSuccess();
    void ReportNeo4jFailure(Exception exception);
}

public sealed class NoOpNeo4jAvailabilityReporter : INeo4jAvailabilityReporter
{
    public static NoOpNeo4jAvailabilityReporter Instance { get; } = new();

    private NoOpNeo4jAvailabilityReporter()
    {
    }

    public void ReportNeo4jSuccess()
    {
    }

    public void ReportNeo4jFailure(Exception exception)
    {
    }
}

public static class Neo4jAvailabilityFailureDetector
{
    private const string ConnectionPoolAcquisitionMessageFragment = "failed to obtain a connection from pool";

    private static readonly string[] ConnectivityMessageFragments =
    [
        ConnectionPoolAcquisitionMessageFragment,
        "failed to connect",
        "could not create connection",
        "unable to connect to database",
        "failed to connect to any write server",
        "no routing servers available",
        "no read server available",
        "no write server available"
    ];

    public static bool IsUnavailableException(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is ServiceUnavailableException or SessionExpiredException)
            {
                return true;
            }

            if (current is ClientException && ContainsConnectivityMessage(current.Message))
            {
                return true;
            }

            if (ContainsConnectivityMessage(current.Message))
            {
                return true;
            }
        }

        return false;
    }

    public static bool IsConnectionPoolAcquisitionTimeout(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is ClientException && ContainsConnectionPoolAcquisitionMessage(current.Message))
            {
                return true;
            }

            if (ContainsConnectionPoolAcquisitionMessage(current.Message))
            {
                return true;
            }
        }

        return false;
    }

    private static bool ContainsConnectivityMessage(string? message)
    {
        if (string.IsNullOrWhiteSpace(message))
        {
            return false;
        }

        foreach (var fragment in ConnectivityMessageFragments)
        {
            if (message.Contains(fragment, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static bool ContainsConnectionPoolAcquisitionMessage(string? message)
        => !string.IsNullOrWhiteSpace(message)
           && message.Contains(ConnectionPoolAcquisitionMessageFragment, StringComparison.OrdinalIgnoreCase);
}
