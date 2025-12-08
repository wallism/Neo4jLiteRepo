using Neo4j.Driver;

namespace Neo4jLiteRepo.Interfaces;

/// <summary>
/// CRUD operations for Neo4j graph nodes.
/// </summary>
public interface INeo4jNodeCrudRepository
{
    /// <summary>
    /// Convenience method - creates its own session
    /// </summary>
    Task<IResultSummary> UpsertNode<T>(T node, CancellationToken ct = default) where T : GraphNode;

    /// <summary>
    /// Use provided session (for batching operations or custom session config)
    /// </summary>
    Task<IResultSummary> UpsertNode<T>(T node, IAsyncSession session, CancellationToken ct = default) where T : GraphNode;

    /// <summary>
    /// Use provided transaction (for multi-operation transactions)
    /// </summary>
    Task<IResultSummary> UpsertNode<T>(T node, IAsyncTransaction tx, CancellationToken ct = default) where T : GraphNode;

    /// <summary>
    /// Upserts a collection of nodes, creating its own session. Returns the individual write cursors (one per node) for optional inspection.
    /// </summary>
    Task<IEnumerable<IResultSummary>> UpsertNodes<T>(IEnumerable<T> nodes) where T : GraphNode;

    /// <summary>
    /// Upserts a collection of nodes with cancellation support, creating its own session.
    /// </summary>
    Task<IEnumerable<IResultSummary>> UpsertNodes<T>(IEnumerable<T> nodes, CancellationToken ct) where T : GraphNode;

    /// <summary>
    /// Upserts a collection of nodes using an existing session (callers can batch multiple operations per session for efficiency).
    /// </summary>
    Task<IEnumerable<IResultSummary>> UpsertNodes<T>(IEnumerable<T> nodes, IAsyncSession session, CancellationToken ct = default) where T : GraphNode;

    /// <summary>
    /// Upserts a collection of nodes using an existing transaction (ensures atomic multi-node upsert behavior when desired).
    /// </summary>
    Task<IEnumerable<IResultSummary>> UpsertNodes<T>(IEnumerable<T> nodes, IAsyncTransaction tx, CancellationToken ct = default) where T : GraphNode;

    /// <summary>
    /// Loads a single node of type <typeparamref name="T"/> by its primary key value (usually Id) and populates any outgoing relationship id lists.
    /// </summary>
    Task<T?> LoadAsync<T>(string id, CancellationToken ct = default) where T : GraphNode, new();

    /// <summary>
    /// Loads a single node with optional edge object loading.
    /// </summary>
    Task<T?> LoadAsync<T>(string id, bool includeEdgeObjects, IEnumerable<string>? includeEdges, CancellationToken ct = default)
        where T : GraphNode, new();

    /// <summary>
    /// Loads all nodes of type <typeparamref name="T"/> and populates any outgoing relationship id lists.
    /// </summary>
    Task<IReadOnlyList<T>> LoadAllAsync<T>(CancellationToken ct = default) where T : GraphNode, new();

    /// <summary>
    /// Loads all nodes of a given type with pagination support.
    /// </summary>
    Task<IReadOnlyList<T>> LoadAllAsync<T>(int skip, int take, CancellationToken ct = default) where T : GraphNode, new();

    /// <summary>
    /// Loads all nodes with pagination and optional edge object loading.
    /// </summary>
    Task<IReadOnlyList<T>> LoadAllAsync<T>(int skip, int take, bool includeEdgeObjects, IEnumerable<string>? includeEdges, CancellationToken ct = default)
        where T : GraphNode, new();

    /// <summary>
    /// Delete node - DETACH DELETE so all edges are also removed.
    /// </summary>
    Task<IResultSummary> DetachDeleteAsync<T>(T node, CancellationToken ct = default)
        where T : GraphNode, new();

    /// <summary>
    /// Delete node using provided transaction.
    /// </summary>
    Task<IResultSummary> DetachDeleteAsync<T>(T node, IAsyncTransaction tx, CancellationToken ct = default)
        where T : GraphNode, new();

    /// <summary>
    /// Delete node by primary key value.
    /// </summary>
    Task<IResultSummary> DetachDeleteAsync<T>(string pkValue, CancellationToken ct = default)
        where T : GraphNode, new();

    /// <summary>
    /// Delete node by primary key value using provided transaction.
    /// </summary>
    Task<IResultSummary> DetachDeleteAsync<T>(string pkValue, IAsyncTransaction tx, CancellationToken ct = default)
        where T : GraphNode, new();

    /// <summary>
    /// Delete multiple nodes.
    /// </summary>
    Task<IResultSummary> DetachDeleteManyAsync<T>(List<T> nodes, CancellationToken ct = default)
        where T : GraphNode, new();

    /// <summary>
    /// Delete multiple nodes using provided transaction.
    /// </summary>
    Task<IResultSummary> DetachDeleteManyAsync<T>(List<T> nodes, IAsyncTransaction tx, CancellationToken ct = default)
        where T : GraphNode, new();

    /// <summary>
    /// Delete multiple nodes by IDs.
    /// </summary>
    Task<IResultSummary> DetachDeleteManyAsync<T>(List<string> ids, CancellationToken ct = default)
        where T : GraphNode, new();

    /// <summary>
    /// Delete multiple nodes by IDs using provided transaction.
    /// </summary>
    Task<IResultSummary> DetachDeleteManyAsync<T>(List<string> ids, IAsyncTransaction tx, CancellationToken ct = default)
        where T : GraphNode, new();

    /// <summary>
    /// Deletes (DETACH DELETE) nodes by id creating its own session/transaction.
    /// </summary>
    Task DetachDeleteNodesByIdsAsync(string label, IEnumerable<string> ids, CancellationToken ct = default);

    /// <summary>
    /// Deletes (DETACH DELETE) nodes by id using an existing session.
    /// </summary>
    Task DetachDeleteNodesByIdsAsync(string label, IEnumerable<string> ids, IAsyncSession session, CancellationToken ct = default);

    /// <summary>
    /// Deletes nodes by id using the provided transaction.
    /// </summary>
    Task DetachDeleteNodesByIdsAsync(string label, IEnumerable<string> ids, IAsyncTransaction tx, CancellationToken ct = default);
}
