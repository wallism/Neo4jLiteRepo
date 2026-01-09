using Neo4j.Driver;
using Neo4jLiteRepo.Models;

namespace Neo4jLiteRepo.Interfaces;

/// <summary>
/// Relationship/edge operations for Neo4j graph nodes.
/// </summary>
public interface INeo4jRelationshipRepository
{
    /// <summary>
    /// Merges a relationship between two nodes in the Neo4j database.
    /// </summary>
    Task MergeRelationshipAsync(GraphNode fromNode, string rel, GraphNode toNode, CancellationToken ct = default);

    /// <summary>
    /// Merges a relationship between two nodes using the provided transaction.
    /// </summary>
    Task MergeRelationshipAsync(GraphNode fromNode, string rel, GraphNode toNode, IAsyncTransaction tx, CancellationToken ct = default);

    /// <summary>
    /// Upserts relationships for a collection of nodes in the Neo4j database.
    /// </summary>
    Task<bool> UpsertRelationshipsAsync<T>(IEnumerable<T> fromNodes) where T : GraphNode;

    /// <summary>
    /// Upserts relationships for a single node in the Neo4j database.
    /// </summary>
    Task<bool> UpsertRelationshipsAsync<T>(T nodes) where T : GraphNode;

    /// <summary>
    /// Upserts relationships for a node using the provided session.
    /// </summary>
    Task<bool> UpsertRelationshipsAsync<T>(T nodes, IAsyncSession session) where T : GraphNode;

    /// <summary>
    /// Deletes a relationship of the specified type and direction between two nodes.
    /// </summary>
    Task DeleteRelationshipAsync(GraphNode fromNode, string rel, GraphNode toNode, EdgeDirection direction, CancellationToken ct = default);

    /// <summary>
    /// Deletes a relationship of the specified type and direction between two nodes using the provided transaction.
    /// </summary>
    Task DeleteRelationshipAsync(GraphNode fromNode, string rel, GraphNode toNode, EdgeDirection direction, IAsyncTransaction tx, CancellationToken ct = default);

    /// <summary>
    /// Deletes multiple edges as specified using the provided transaction.
    /// </summary>
    Task DeleteEdgesAsync(IEnumerable<EdgeDeleteSpec> specs, IAsyncTransaction tx, CancellationToken ct = default);

    /// <summary>
    /// Deletes all relationships of a given type and direction from the specified node using the provided transaction.
    /// </summary>
    Task<IResultSummary> DeleteRelationshipsOfTypeFromAsync(GraphNode fromNode, string rel, EdgeDirection direction,
        IAsyncTransaction tx, CancellationToken ct = default);

    /// <summary>
    /// Loads related nodes of type <typeparamref name="TRelated"/> reachable from a source node.
    /// </summary>
    Task<IReadOnlyList<TRelated>> LoadRelatedNodesAsync<TSource, TRelated>(string sourceId, string relationshipTypes, int minHops = 1, int maxHops = 4, IAsyncTransaction? tx = null,
        CancellationToken ct = default)
        where TSource : GraphNode, new()
        where TRelated : GraphNode, new();

    /// <summary>
    /// Loads the distinct ids of related nodes.
    /// </summary>
    Task<IReadOnlyList<string>> LoadRelatedNodeIdsAsync<TRelated>(GraphNode fromNode, string relationshipTypes, int minHops = 1, int maxHops = 4,
        EdgeDirection direction = EdgeDirection.Outgoing, IAsyncTransaction? tx = null, CancellationToken ct = default)
        where TRelated : GraphNode, new();

    /// <summary>
    /// Traverses from a source node to related nodes via specified relationship path (variable-length pattern matching).
    /// Returns only the target nodes WITHOUT any edge data populated. For nodes WITH edges, use LoadRelatedAsync instead.
    /// </summary>
    Task<IReadOnlyList<TRelated>> LoadNodesViaPathNoEdgesAsync<TSource, TRelated>(string sourceId, string relationshipTypes, int minHops = 1, int maxHops = 4, IAsyncTransaction? tx = null,
        CancellationToken ct = default)
        where TSource : GraphNode, new()
        where TRelated : GraphNode, new();

    /// <summary>
    /// Traverses from a source node and returns only the distinct IDs of related nodes (no full node hydration, no edges).
    /// Lightweight variant useful for relationship/cascade operations where you only need IDs.
    /// </summary>
    Task<IReadOnlyList<string>> LoadNodeIdsViaPathNoEdgesAsync<TRelated>(GraphNode fromNode, string relationshipTypes, int minHops = 1, int maxHops = 4,
        EdgeDirection direction = EdgeDirection.Outgoing, IAsyncTransaction? tx = null, CancellationToken ct = default)
        where TRelated : GraphNode, new();
}
