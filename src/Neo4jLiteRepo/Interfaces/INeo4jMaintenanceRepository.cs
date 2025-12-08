using Neo4j.Driver;

namespace Neo4jLiteRepo.Interfaces;

/// <summary>
/// Maintenance and cleanup operations for Neo4j.
/// </summary>
public interface INeo4jMaintenanceRepository
{
    /// <summary>
    /// Removes orphan nodes (nodes with zero relationships) for label derived from <typeparamref name="T"/>.
    /// Runs in its own write transaction. Returns number of deleted nodes.
    /// </summary>
    Task<int> RemoveOrphansAsync<T>(CancellationToken ct = default) where T : GraphNode, new();

    /// <summary>
    /// Removes orphan nodes using an existing transaction.
    /// </summary>
    Task<int> RemoveOrphansAsync<T>(IAsyncTransaction tx, CancellationToken ct = default) where T : GraphNode, new();

    /// <summary>
    /// Removes orphan nodes using an existing session.
    /// </summary>
    Task<int> RemoveOrphansAsync<T>(IAsyncSession session, CancellationToken ct = default) where T : GraphNode, new();
}
