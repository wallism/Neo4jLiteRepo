using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Attributes;
using Neo4jLiteRepo.Exceptions;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.Models;
using System.Reflection;

namespace Neo4jLiteRepo;

/// <summary>
/// CRUD operations for Neo4j nodes.
/// </summary>
public partial class Neo4jGenericRepo
{
    /// <summary>
    /// Default batch size for bulk operations. Balances memory usage with round-trip overhead.
    /// </summary>
    private const int DefaultBatchSize = 500;

    #region UpsertNode

    /// <inheritdoc/>
    public async Task<IResultSummary> UpsertNode<T>(T node, CancellationToken ct = default) where T : GraphNode
    {
        await using var session = StartSession();
        return await UpsertNode(node, session, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Upserts a node using an existing session. Caller is responsible for disposing the session.
    /// </summary>
    public async Task<IResultSummary> UpsertNode<T>(T node, IAsyncSession session, CancellationToken ct = default) where T : GraphNode
    {
        _logger.LogInformation("({label}:{node})", node.LabelName, node.DisplayName);
        await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
        try
        {
            var result = await UpsertNode(node, tx, ct).ConfigureAwait(false);
            await tx.CommitAsync().ConfigureAwait(false);
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to upsert node {Label}:{DisplayName}", node.LabelName, node.DisplayName);
            await tx.RollbackAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task<IResultSummary> UpsertNode<T>(T node, IAsyncTransaction tx, CancellationToken ct = default) where T : GraphNode
    {
        ct.ThrowIfCancellationRequested();
        var cypher = BuildUpsertNodeQuery(node);
        _logger.LogInformation("upsert ({label}:{pk})", node.LabelName, node.GetPrimaryKeyValue());
        return await tx.RunWriteAsync(cypher.Query, cypher.Parameters).ConfigureAwait(false);
    }

    #endregion

    #region UpsertNodes

    /// <inheritdoc/>
    public async Task<IEnumerable<IResultSummary>> UpsertNodes<T>(IEnumerable<T> nodes) where T : GraphNode
    {
        await using var session = StartSession();
        return await UpsertNodes(nodes, session, CancellationToken.None).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<IEnumerable<IResultSummary>> UpsertNodes<T>(IEnumerable<T> nodes, CancellationToken ct) where T : GraphNode
    {
        await using var session = StartSession();
        return await UpsertNodes(nodes, session, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Upserts nodes using an existing session. Caller is responsible for disposing the session.
    /// </summary>
    public async Task<IEnumerable<IResultSummary>> UpsertNodes<T>(IEnumerable<T> nodes, IAsyncSession session, CancellationToken ct = default) where T : GraphNode
    {
        await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
        try
        {
            var result = await UpsertNodes(nodes, tx, ct);
            await tx.CommitAsync().ConfigureAwait(false);
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to upsert nodes");
            try
            {
                await tx.RollbackAsync().ConfigureAwait(false);
            }
            catch (Exception rollbackEx)
            {
                _logger.LogWarning(rollbackEx, "Rollback failed after upsert nodes error");
            }

            throw;
        }
    }

    /// <inheritdoc/>
    public async Task<IEnumerable<IResultSummary>> UpsertNodes<T>(IEnumerable<T> nodes, IAsyncTransaction tx, CancellationToken ct = default) where T : GraphNode
    {
        List<IResultSummary> results = [];
        foreach (var node in nodes)
        {
            // UpsertNode handles its own cancellation check; no need to throw each iteration here.
            var cursor = await UpsertNode(node, tx, ct).ConfigureAwait(false);
            results.Add(cursor);
        }

        return results;
    }

    #endregion

    #region LoadAsync

    /// <inheritdoc/>
    public async Task<T?> LoadAsync<T>(string id, CancellationToken ct = default) where T : GraphNode, new()
    {
        ct.ThrowIfCancellationRequested();
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("id required", nameof(id));

        var temp = new T();
        var label = temp.LabelName;
        var pkName = temp.GetPrimaryKeyName();

        var relationships = GetRelationshipMetadata(typeof(T));
        var query = BuildLoadQuery(label, pkName, relationships, $"{{ {pkName}: $id }}", null, null, false, null);

        await using var session = StartSession();
        try
        {
            var records = await session.ExecuteReadAsync(async tx =>
            {
                var cursor = await tx.RunAsync(query, new { id });
                return await cursor.ToListAsync(cancellationToken: ct);
            });

            if (records.Count is 0) return null;
            var record = records[0];
            if (!record.Keys.Contains("n"))
                throw new RepositoryException("Load query did not return alias 'n'.", query, ["id"], null);

            var node = record["n"].As<INode>();
            var entity = MapNodeToObject<T>(node);
            MapRelationshipLists(record, entity, relationships);
            // Edge objects disabled in this overload to preserve existing behavior.
            return entity;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed loading node {Label}:{Id}", label, id);
            throw new RepositoryException($"Failed loading node {label}:{id}", query, ["id"], ex);
        }
    }

    /// <summary>
    /// Loads a single node of the specified type by its primary key, also populates outgoing relationship List&lt;string&gt; properties.
    /// Opt in to load edge-object maps for relationships that have an associated edge object type.
    /// </summary>
    public async Task<T?> LoadAsync<T>(string id, bool includeEdgeObjects, IEnumerable<string>? includeEdges, CancellationToken ct = default)
        where T : GraphNode, new()
    {
        ct.ThrowIfCancellationRequested();
        if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("id required", nameof(id));

        var temp = new T();
        var label = temp.LabelName;
        var pkName = temp.GetPrimaryKeyName();

        var rels = GetRelationshipMetadata(typeof(T));
        var includeSet = includeEdges != null ? new HashSet<string>(includeEdges, StringComparer.OrdinalIgnoreCase) : null;

        var query = BuildLoadQuery(label, pkName, rels, $"{{ {pkName}: $id }}", null, null, includeEdgeObjects, includeSet);

        await using var session = StartSession();
        try
        {
            var records = await session.ExecuteReadAsync(async tx =>
            {
                var cursor = await tx.RunAsync(query, new { id });
                return await cursor.ToListAsync(cancellationToken: ct);
            });

            if (records.Count is 0) return null;
            var record = records[0];
            var node = record["n"].As<INode>();
            var entity = MapNodeToObject<T>(node);
            MapRelationshipLists(record, entity, rels);
            if (includeEdgeObjects)
                MapEdgeObjects(record, entity, rels);
            return entity;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed loading node {Label}:{Id}", label, id);
            throw;
        }
    }

    #endregion

    #region LoadAllAsync

    /// <summary>
    /// Loads all nodes of a given type and populates outgoing relationship List&lt;string&gt; properties defined with <see cref="NodeRelationshipAttribute{T}"/>.
    /// Only related node primary key values are populated (not full node objects) to keep the load lightweight.
    /// </summary>
    /// <typeparam name="T">Concrete GraphNode type to load.</typeparam>
    /// <param name="ct">Cancellation token.</param>
    public async Task<IReadOnlyList<T>> LoadAllAsync<T>(CancellationToken ct = default) where T : GraphNode, new()
    {
        return await LoadAllAsync<T>(0, int.MaxValue, ct);
    }

    /// <summary>
    /// Loads all nodes of a given type and populates outgoing relationship List&lt;string&gt; properties defined with <see cref="NodeRelationshipAttribute{T}"/>.
    /// Only related node primary key values are populated (not full node objects) to keep the load lightweight.
    /// Supports pagination via skip/take.
    /// </summary>
    /// <typeparam name="T">Concrete GraphNode type to load.</typeparam>
    /// <param name="skip">Number of records to skip (for pagination).</param>
    /// <param name="take">Maximum number of records to take (for pagination).</param>
    /// <param name="ct">Cancellation token.</param>
    public async Task<IReadOnlyList<T>> LoadAllAsync<T>(int skip, int take, CancellationToken ct = default) where T : GraphNode, new()
    {
        ct.ThrowIfCancellationRequested();
        var temp = new T();
        var label = temp.LabelName;
        var pkName = temp.GetPrimaryKeyName();
        var relationships = GetRelationshipMetadata(typeof(T));

        var query = BuildLoadQuery(label, pkName, relationships, null, skip, take, false, null);

        var parameters = new Dictionary<string, object>();
        if (query.Contains("$skip")) parameters["skip"] = skip;
        if (query.Contains("$take")) parameters["take"] = take;

        await using var session = StartSession();
        try
        {
            var records = await session.ExecuteReadAsync(async tx =>
            {
                var cursor = await tx.RunAsync(query, parameters);
                return await cursor.ToListAsync(cancellationToken: ct);
            });

            if (records.Count is 0) return [];
            var results = new List<T>(records.Count);

            foreach (var record in records)
            {
                if (!record.Keys.Contains("n")) continue;
                var node = record["n"].As<INode>();
                var entity = MapNodeToObject<T>(node);
                MapRelationshipLists(record, entity, relationships);
                // Edge objects disabled in this overload to preserve existing behavior.
                results.Add(entity);
            }

            return results;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed loading all nodes for {Label}", label);
            throw new RepositoryException($"Failed loading all nodes for {label}", query, [], ex);
        }
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<T>> LoadAllAsync<T>(int skip, int take, bool includeEdgeObjects, IEnumerable<string>? includeEdges, CancellationToken ct = default)
        where T : GraphNode, new()
    {
        ct.ThrowIfCancellationRequested();
        var temp = new T();
        var label = temp.LabelName;
        var pkName = temp.GetPrimaryKeyName();

        var rels = GetRelationshipMetadata(typeof(T));
        var includeSet = includeEdges != null ? new HashSet<string>(includeEdges, StringComparer.OrdinalIgnoreCase) : null;

        var query = BuildLoadQuery(label, pkName, rels, null, skip, take, includeEdgeObjects, includeSet);

        var parameters = new Dictionary<string, object>();
        if (query.Contains("$skip")) parameters["skip"] = skip;
        if (query.Contains("$take")) parameters["take"] = take;

        await using var session = StartSession();
        try
        {
            var records = await session.ExecuteReadAsync(async tx =>
            {
                var cursor = await tx.RunAsync(query, parameters);
                return await cursor.ToListAsync(cancellationToken: ct);
            });

            if (records.Count is 0) return [];
            var results = new List<T>(records.Count);

            foreach (var record in records)
            {
                if (!record.Keys.Contains("n")) continue;
                var node = record["n"].As<INode>();
                var entity = MapNodeToObject<T>(node);
                MapRelationshipLists(record, entity, rels);
                if (includeEdgeObjects)
                    MapEdgeObjects(record, entity, rels);
                results.Add(entity);
            }

            return results;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed loading all nodes for {Label}", label);
            throw;
        }
    }

    #endregion

    #region DetachDelete

    /// <inheritdoc/>
    public async Task<IResultSummary> DetachDeleteAsync<T>(T node, CancellationToken ct = default)
        where T : GraphNode, new()
    {
        if (node == null) throw new ArgumentNullException(nameof(node));

        await using var session = StartSession();
        await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
        try
        {
            var result = await DetachDeleteAsync(node, tx, ct).ConfigureAwait(false);
            await tx.CommitAsync().ConfigureAwait(false);
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to detach delete node");
            try
            {
                await tx.RollbackAsync().ConfigureAwait(false);
            }
            catch (Exception rollbackEx) { _logger.LogWarning(rollbackEx, "Rollback failed after detach delete error"); }
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task<IResultSummary> DetachDeleteAsync<T>(T node, IAsyncTransaction tx, CancellationToken ct = default)
        where T : GraphNode, new()
    {
        if (node == null) throw new ArgumentNullException(nameof(node));
        var pkValue = node.GetPrimaryKeyValue();
        return await DetachDeleteAsync<T>(pkValue, tx, ct);
    }

    /// <inheritdoc/>
    public async Task<IResultSummary> DetachDeleteAsync<T>(string pkValue, CancellationToken ct = default)
        where T : GraphNode, new()
    {
        await using var session = StartSession();
        await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
        try
        {
            var result = await DetachDeleteAsync<T>(pkValue, tx, ct).ConfigureAwait(false);
            await tx.CommitAsync().ConfigureAwait(false);
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to detach delete node by primary key {PkValue}", pkValue);
            try
            {
                await tx.RollbackAsync().ConfigureAwait(false);
            }
            catch (Exception rollbackEx) { _logger.LogWarning(rollbackEx, "Rollback failed after detach delete error"); }
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task<IResultSummary> DetachDeleteAsync<T>(string pkValue, IAsyncTransaction tx, CancellationToken ct = default)
        where T : GraphNode, new()
    {
        ct.ThrowIfCancellationRequested();
        if (tx == null) throw new ArgumentNullException(nameof(tx));
        if (string.IsNullOrWhiteSpace(pkValue)) throw new ArgumentException("Primary key value required", nameof(pkValue));

        var pkName = GraphNode.GetPrimaryKeyName<T>();
        var labelName = GraphNode.GetLabelName<T>();

        var cypher = $$"""
                       MATCH (n:{{labelName}} {{{pkName}}: $pkValue })
                       DETACH DELETE n
                       """;
        var parameters = new Dictionary<string, object> { { "pkValue", pkValue } };
        try
        {
            return await tx.RunWriteAsync(cypher, parameters);
        }
        catch (Exception ex) // catch to log
        {
            _logger.LogError(ex, "running cypher: {query}", cypher);
            throw;
        }
    }

    #endregion

    #region DetachDeleteMany

    /// <inheritdoc/>
    public async Task<IResultSummary> DetachDeleteManyAsync<T>(List<T> nodes, CancellationToken ct = default)
        where T : GraphNode, new()
    {
        await using var session = StartSession();
        await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
        try
        {
            var result = await DetachDeleteManyAsync(nodes, tx, ct).ConfigureAwait(false);
            await tx.CommitAsync().ConfigureAwait(false);
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to detach delete many nodes");
            try
            {
                await tx.RollbackAsync().ConfigureAwait(false);
            }
            catch (Exception rollbackEx) { _logger.LogWarning(rollbackEx, "Rollback failed after detach delete many error"); }
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task<IResultSummary> DetachDeleteManyAsync<T>(List<T> nodes, IAsyncTransaction tx, CancellationToken ct = default)
        where T : GraphNode, new()
    {
        if (nodes == null) throw new ArgumentNullException(nameof(nodes));
        var ids = nodes.Select(n => n.GetPrimaryKeyValue()).ToList();
        return await DetachDeleteManyAsync<T>(ids, tx, ct);
    }

    /// <inheritdoc/>
    public async Task<IResultSummary> DetachDeleteManyAsync<T>(List<string> ids, CancellationToken ct = default)
        where T : GraphNode, new()
    {
        await using var session = StartSession();
        await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
        try
        {
            var result = await DetachDeleteManyAsync<T>(ids, tx, ct).ConfigureAwait(false);
            await tx.CommitAsync().ConfigureAwait(false);
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to detach delete many nodes by ids");
            try
            {
                await tx.RollbackAsync().ConfigureAwait(false);
            }
            catch (Exception rollbackEx) { _logger.LogWarning(rollbackEx, "Rollback failed after detach delete many error"); }
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task<IResultSummary> DetachDeleteManyAsync<T>(List<string> ids, IAsyncTransaction tx, CancellationToken ct = default)
        where T : GraphNode, new()
    {
        ct.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(tx);
        if (ids.Count is 0) throw new ArgumentException("At least one id is required", nameof(ids));

        var pkName = GraphNode.GetPrimaryKeyName<T>();
        var labelName = GraphNode.GetLabelName<T>();

        var cypher = $"""
                      MATCH (n:{labelName})
                      WHERE n.{pkName} IN $pkValues
                      DETACH DELETE n
                      """;

        var parameters = new Dictionary<string, object>
        {
            { "pkValues", ids }
        };

        return await tx.RunWriteAsync(cypher, parameters);
    }

    #endregion

    #region DetachDeleteNodesByIds

    /// <summary>
    /// detach delete nodes by id.creates its own session &amp; transaction.
    /// Mirrors the pattern used by UpsertNodes convenience overloads.
    /// </summary>
    /// <param name="label">Node label</param>
    /// <param name="ids">Primary key values (id property)</param>
    /// <param name="ct">Cancellation token</param>
    public async Task DetachDeleteNodesByIdsAsync(string label, IEnumerable<string> ids, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await using var session = StartSession();
        await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
        try
        {
            await DetachDeleteNodesByIdsAsync(label, ids, tx, ct).ConfigureAwait(false);
            await tx.CommitAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to detach delete nodes by ids for label {Label}", label);
            try
            {
                await tx.RollbackAsync().ConfigureAwait(false);
            }
            catch (Exception rollbackEx)
            {
                _logger.LogWarning(rollbackEx, "Rollback failed after detach delete nodes by ids error");
            }

            throw;
        }
    }

    /// <summary>
    /// Deletes (DETACH DELETE) nodes by id using an existing session (wraps in a transaction internally).
    /// </summary>
    /// <param name="label">Node label</param>
    /// <param name="ids">Primary key values (id property)</param>
    /// <param name="session">Existing async session</param>
    /// <param name="ct">Cancellation token</param>
    public async Task DetachDeleteNodesByIdsAsync(string label, IEnumerable<string> ids, IAsyncSession session, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        ct.ThrowIfCancellationRequested();
        await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
        try
        {
            await DetachDeleteNodesByIdsAsync(label, ids, tx, ct).ConfigureAwait(false);
            await tx.CommitAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to detach delete nodes by ids for label {Label} (session overload)", label);
            try
            {
                await tx.RollbackAsync().ConfigureAwait(false);
            }
            catch (Exception rollbackEx)
            {
                _logger.LogWarning(rollbackEx, "Rollback failed after detach delete nodes by ids error");
            }

            throw;
        }
    }

    /// <summary>
    /// Deletes (DETACH DELETE - i.e. all relationships connected to the node will also be deleted)
    /// nodes of the specified label whose id property matches any of the provided ids.
    /// Uses batching + UNWIND for large collections to stay within query / memory limits. Assumes identity property 'id'.
    /// </summary>
    public async Task DetachDeleteNodesByIdsAsync(string label, IEnumerable<string> ids, IAsyncTransaction tx, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(tx);
        ArgumentException.ThrowIfNullOrWhiteSpace(label);
        if (!_labelValidationRegex.IsMatch(label))
            throw new ArgumentException($"Invalid label '{label}'. Only A-Z, a-z, 0-9 and '_' allowed.", nameof(label));

        if (!IsInDetachDeleteWhitelist(label))
        {
            _logger.LogWarning("Label {Label} is not in the whitelist for DetachDeleteNodesByIdsAsync; skipping delete operation", label);
            return;
        }

        var idList = ids?
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => s.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList() ?? [];
        if (idList.Count is 0)
        {
            _logger.LogInformation("DeleteNodesByIdsAsync called with 0 ids for label {Label}; nothing to do", label);
            return;
        }
        var total = idList.Count;
        var processed = 0;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        for (var i = 0; i < idList.Count; i += DefaultBatchSize)
        {
            ct.ThrowIfCancellationRequested();
            var batch = idList.Skip(i).Take(DefaultBatchSize).ToList();
            var query = $$"""
                          UNWIND $ids AS id
                          MATCH (n:{{label}} { id: id })
                          DETACH DELETE n
                          """; // label safe after regex validation
            try
            {
                await ExecuteWriteQuery(tx, query, new { ids = batch });
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Failed deleting nodes batch {Start}-{End} of {Total} for label {Label}", i + 1, i + batch.Count, total, label);
                throw;
            }

            processed += batch.Count;
        }

        sw.Stop();
        _logger.LogInformation("DeleteNodesByIdsAsync deleted {Count} nodes for label {Label} in {ElapsedMs}ms (batches of {BatchSize})", processed, label, sw.ElapsedMilliseconds, DefaultBatchSize);
    }

    #endregion

    #region ExecuteWriteAsync

    /// <inheritdoc/>
    public async Task<IResultSummary> ExecuteWriteAsync(string query, IDictionary<string, object>? parameters = null)
    {
        await using var session = StartSession();
        return await ExecuteWriteAsync(query, parameters, session);
    }

    /// <inheritdoc/>
    public async Task<IResultSummary> ExecuteWriteAsync(string query, IDictionary<string, object>? parameters, IAsyncSession session)
    {
        return await session.ExecuteWriteAsync(async tx =>
        {
            return await ExecuteWriteQuery(tx, query, parameters ?? new Dictionary<string, object>());
        });
    }

    #endregion
}
