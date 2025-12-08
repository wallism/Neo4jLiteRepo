using Neo4j.Driver;

namespace Neo4jLiteRepo.Interfaces;

/// <summary>
/// Session and transaction management for Neo4j.
/// </summary>
public interface INeo4jSessionRepository
{
    /// <summary>
    /// Starts and returns a new asynchronous Neo4j session.
    /// </summary>
    IAsyncSession StartSession();
}
