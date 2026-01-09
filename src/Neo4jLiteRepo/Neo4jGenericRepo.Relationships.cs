using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Attributes;
using Neo4jLiteRepo.Exceptions;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.Models;
using System.Reflection;

namespace Neo4jLiteRepo;

/// <summary>
/// Relationship operations for Neo4j nodes.
/// </summary>
public partial class Neo4jGenericRepo
{
    #region MergeRelationship

    /// <summary>
    /// Merges (creates if missing) a relationship of type <paramref name="rel"/> from one node to another.
    /// </summary>
    public async Task MergeRelationshipAsync(GraphNode fromNode, string rel, GraphNode toNode, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await using var session = StartSession();
        await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
        try
        {
            // Use new refactored method below
            await MergeRelationshipAsync(fromNode, rel, toNode, tx, ct);
            await tx.CommitAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to merge relationship {Rel} from {FromLabel} to {ToLabel}", rel, fromNode.LabelName, toNode.LabelName);
            try
            {
                await tx.RollbackAsync().ConfigureAwait(false);
            }
            catch (Exception rollbackEx)
            {
                _logger.LogWarning(rollbackEx, "Rollback failed after merge relationship error");
            }

            throw;
        }
    }

    /// <summary>
    /// Merges (creates if missing) a relationship of type <paramref name="rel"/> from one node to another.
    /// </summary>
    public async Task MergeRelationshipAsync(GraphNode fromNode, string rel, GraphNode toNode, IAsyncTransaction tx, CancellationToken ct = default)
    {
        if (tx == null) throw new ArgumentNullException(nameof(tx));
        ct.ThrowIfCancellationRequested();
        ValidateRel(rel, nameof(rel));
        var fromPkValue = fromNode.GetPrimaryKeyValue();
        var toPkValue = toNode.GetPrimaryKeyValue();
        var fromPkName = fromNode.GetPrimaryKeyName();
        var toPkName = toNode.GetPrimaryKeyName();

        if (string.IsNullOrWhiteSpace(fromPkValue)) throw new ArgumentException($"{fromPkName} required", fromPkName);
        if (string.IsNullOrWhiteSpace(toPkValue)) throw new ArgumentException($"{toPkName} required", toPkName);

        var cypher = $$"""
            MATCH (f:{{fromNode.LabelName}} { {{fromPkName}}: $fromPkValue })
            MATCH (t:{{toNode.LabelName}} { {{toPkName}}: $toPkValue })
            MERGE (f)-[r:{{rel}}]->(t)
            RETURN r
        """;

        var parameters = new Dictionary<string, object>
        {
            { "fromPkValue", fromPkValue },
            { "toPkValue", toPkValue }
        };
        try
        {
            await ExecuteWriteQuery(tx, cypher, parameters);
            _logger.LogInformation("MERGE {FromLabel}:{FromPkValue}-[{Rel}]->{ToLabel}:{ToPkValue}", fromNode.LabelName, fromPkValue, rel, toNode.LabelName, toPkValue);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed merging relationship {Rel} {FromLabel}:{FromPkValue}->{ToLabel}:{ToPkValue}", rel, fromNode.LabelName, fromPkValue, toNode.LabelName, toPkValue);
            throw;
        }
    }

    #endregion

    #region DeleteRelationship

    /// <summary>
    /// Deletes a single relationship of the specified type between two nodes (specify direction).
    /// Session-managed overload to mirror MergeRelationshipAsync.
    /// </summary>
    public async Task DeleteRelationshipAsync(GraphNode fromNode, string rel, GraphNode toNode, EdgeDirection direction, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await using var session = StartSession();
        await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
        try
        {
            await DeleteRelationshipAsync(fromNode, rel, toNode, direction, tx, ct).ConfigureAwait(false);
            await tx.CommitAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete relationship {Rel} from {FromLabel} to {ToLabel}", rel, fromNode.LabelName, toNode.LabelName);
            try { await tx.RollbackAsync().ConfigureAwait(false); }
            catch (Exception rollbackEx) { _logger.LogWarning(rollbackEx, "Rollback failed after delete relationship error"); }
            throw;
        }
    }

    /// <summary>
    /// Deletes a single relationship of the specified type between two nodes (specify direction).
    /// Transaction-based overload that mirrors the PK-based matching used by MergeRelationshipAsync.
    /// </summary>
    public async Task DeleteRelationshipAsync(GraphNode fromNode, string rel, GraphNode toNode, EdgeDirection direction, IAsyncTransaction tx, CancellationToken ct = default)
    {
        if (tx == null) throw new ArgumentNullException(nameof(tx));
        ct.ThrowIfCancellationRequested();
        ValidateRel(rel, nameof(rel));

        var fromPkValue = fromNode.GetPrimaryKeyValue();
        var toPkValue = toNode.GetPrimaryKeyValue();
        var fromPkName = fromNode.GetPrimaryKeyName();
        var toPkName = toNode.GetPrimaryKeyName();

        if (string.IsNullOrWhiteSpace(fromPkValue)) throw new ArgumentException($"{fromPkName} required", fromPkName);
        if (string.IsNullOrWhiteSpace(toPkValue)) throw new ArgumentException($"{toPkName} required", toPkName);

        var pattern = direction switch
        {
            EdgeDirection.Outgoing => $"(f)-[r:{rel}]->(t)",
            EdgeDirection.Incoming => $"(f)<-[r:{rel}]-(t)",
            EdgeDirection.Both => $"(f)-[r:{rel}]-(t)",
            _ => throw new ArgumentException($"Invalid direction: {direction}")
        };

        var cypher = $$"""
            MATCH (f:{{fromNode.LabelName}} { {{fromPkName}}: $fromPkValue })
            MATCH (t:{{toNode.LabelName}} { {{toPkName}}: $toPkValue })
            MATCH {{pattern}}
            DELETE r
        """;

        var parameters = new Dictionary<string, object>
        {
            { "fromPkValue", fromPkValue },
            { "toPkValue", toPkValue }
        };

        try
        {
            await ExecuteWriteQuery(tx, cypher, parameters);
            _logger.LogInformation("Deleted relationship {Rel} {FromLabel}:{FromPkValue} -> {ToLabel}:{ToPkValue}", rel, fromNode.LabelName, fromPkValue, toNode.LabelName, toPkValue);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed deleting relationship {Rel} {FromLabel}:{FromPkValue}->{ToLabel}:{ToPkValue}", rel, fromNode.LabelName, fromPkValue, toNode.LabelName, toPkValue);
            throw;
        }
    }

    #endregion

    #region DeleteEdges

    /// <summary>
    /// Deletes multiple relationships (single edges) in batches. Each spec identifies a potential relationship between two nodes.
    /// Groups by (FromLabel, ToLabel, Rel, Direction) so labels &amp; rel type can be inlined safely (identifiers cannot be parameterized in Cypher).
    /// </summary>
    /// <remarks>
    /// Similar validation &amp; patterns as <see cref="DeleteRelationshipAsync"/> but optimized for bulk removal.
    /// Uses UNWIND with a batch size (default 500) to avoid overwhelming memory in large deletions.
    /// </remarks>
    /// <param name="specs">Collection of relationship delete specifications.</param>
    /// <param name="tx">Active transaction (required).</param>
    /// <param name="ct">Cancellation token.</param>
    public async Task DeleteEdgesAsync(IEnumerable<EdgeDeleteSpec> specs, IAsyncTransaction tx, CancellationToken ct = default)
    {
        if (tx == null) throw new ArgumentNullException(nameof(tx));
        if (specs == null) throw new ArgumentNullException(nameof(specs));

        // Materialize and sanitize list (filter out obviously invalid entries early, while logging).
        var list = specs
            .Where(s => !string.IsNullOrWhiteSpace(s.Rel))
            .Distinct()
            .ToList();

        if (list.Count == 0)
        {
            _logger.LogInformation("DeleteEdgesAsync called with 0 valid specs; nothing to do");
            return;
        }

        const int batchSize = 500; // align with node delete batching
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var total = list.Count;
        var processed = 0;

        // Group by items that can share a single UNWIND query (labels + rel + direction must be constants in text)
        var groups = list.GroupBy(s =>
            new { FromNode = s.FromNode, ToNode = s.ToNode, s.Rel, s.Direction });

        foreach (var group in groups)
        {
            ct.ThrowIfCancellationRequested();
            // Validate identifiers once per group (throws if invalid)
            ValidateRel(group.Key.Rel, nameof(group.Key.Rel));
            var specsInGroup = group.ToList();
            if (specsInGroup.Count == 0)
                continue;

            var sampleSpec = specsInGroup.First();
            var fromPk = sampleSpec.FromNode.GetPrimaryKeyName();
            var toPk = sampleSpec.ToNode.GetPrimaryKeyName();

            // Determine relationship pattern fragment (same logic as single delete variant)
            var pattern = group.Key.Direction switch
            {
                EdgeDirection.Outgoing => $"(f)-[r:{group.Key.Rel}]->(t)",
                EdgeDirection.Incoming => $"(f)<-[r:{group.Key.Rel}]-(t)",
                EdgeDirection.Both => $"(f)-[r:{group.Key.Rel}]-(t)",
                _ => throw new ArgumentException($"Invalid direction {group.Key.Direction}")
            };

            for (var i = 0; i < specsInGroup.Count; i += batchSize)
            {
                ct.ThrowIfCancellationRequested();
                // NOTE: Neo4j .NET driver only supports primitive types, lists and dictionaries for parameters.
                // Using an anonymous type list (new { fromId, toId }) causes a ProtocolException.
                // Convert each pair to a Dictionary<string, object> to satisfy driver constraints.
                var batchPairs = specsInGroup.Skip(i).Take(batchSize)
                    .Select(s => new Dictionary<string, object>
                    {
                        ["fromId"] = s.FromNode.GetPrimaryKeyValue(),
                        ["toId"] = s.ToNode.GetPrimaryKeyValue()
                    })
                    .ToList();

                if (batchPairs.Count == 0)
                    continue;

                var cypher = $$"""
                               UNWIND $pairs AS pair
                               MATCH (f:{{group.Key.FromNode.LabelName}} { {{fromPk}}: pair.fromId })
                               MATCH (t:{{group.Key.ToNode.LabelName}} { {{toPk}}: pair.toId })
                               MATCH {{pattern}}
                               DELETE r
                               """; // identifiers validated

                try
                {
                    await ExecuteWriteQuery(tx, cypher, new { pairs = batchPairs });
                    processed += batchPairs.Count;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(ex,
                        "Failed deleting relationship batch {Start}-{End} of {GroupCount} (TotalSpecs={Total}) {FromLabel}-{Rel}-{ToLabel} Direction={Direction}",
                        i + 1, i + batchPairs.Count, specsInGroup.Count, total, group.Key.FromNode.LabelName, group.Key.Rel, group.Key.ToNode.LabelName, group.Key.Direction);
                    throw;
                }
            }
        }

        sw.Stop();
        _logger.LogInformation("DeleteRelationshipsAsync deleted up to {Processed} relationship specs in {ElapsedMs}ms (batches of {BatchSize})", processed, sw.ElapsedMilliseconds, batchSize);
    }

    #endregion

    #region DeleteRelationshipsOfTypeFrom

    /// <summary>
    /// Deletes all outgoing relationships of a given type from a specific node.
    /// </summary>
    /// <remarks>if unsure about direction, use EdgeDirection.Outgoing</remarks>
    public async Task<IResultSummary> DeleteRelationshipsOfTypeFromAsync(GraphNode fromNode, string rel, EdgeDirection direction,
        IAsyncTransaction tx, CancellationToken ct = default)
    {
        if (tx == null) throw new ArgumentNullException(nameof(tx));
        ct.ThrowIfCancellationRequested();

        var fromPkValue = fromNode.GetPrimaryKeyValue();
        var fromPkName = fromNode.GetPrimaryKeyName();

        ValidateRel(rel, nameof(rel));

        var pattern = direction switch
        {
            EdgeDirection.Outgoing => $"-[r:{rel}]->()",
            EdgeDirection.Incoming => $"<-[r:{rel}]-()",
            EdgeDirection.Both => $"-[r:{rel}]-",
            _ => throw new ArgumentException($"Invalid direction: {direction}")
        };

        var cypher = $$"""
                       MATCH (n:{{fromNode.LabelName}} { {{fromPkName}}: $fromPkValue }){{pattern}}
                       DELETE r
                       """;

        var parameters = new Dictionary<string, object>
        {
            { "fromPkValue", fromPkValue }
        };

        try
        {
            var result = await ExecuteWriteQuery(tx, cypher, parameters);
            if (result.Counters.RelationshipsDeleted > 0)
                _logger.LogInformation("Deleted all ({count}) {Rel} relationships from {Label}:{Id}", result.Counters.RelationshipsDeleted, rel, fromNode.LabelName, fromPkValue);
            return result;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed deleting relationships {Rel} from {Label}:{Id}", rel, fromNode.LabelName, fromPkValue);
            throw;
        }
    }

    #endregion

    #region UpsertRelationships

    /// <inheritdoc/>
    public async Task<bool> UpsertRelationshipsAsync<T>(IEnumerable<T> fromNodes) where T : GraphNode
    {
        await using var session = StartSession();
        foreach (var node in fromNodes)
        {
            var result = await UpsertRelationshipsAsync(node, session).ConfigureAwait(false);
            if (!result)
                return false; // exit on failure. (may want to continue on failure of individual nodes?)
        }

        return true;
    }

    /// <inheritdoc/>
    public async Task<bool> UpsertRelationshipsAsync<T>(T fromNode) where T : GraphNode
    {
        await using var session = StartSession();
        return await UpsertRelationshipsAsync(fromNode, session);
    }

    /// <summary>
    /// Upserts relationships using an existing session. Caller is responsible for disposing the session.
    /// </summary>
    public async Task<bool> UpsertRelationshipsAsync<T>(T fromNode, IAsyncSession session) where T : GraphNode
    {
        var nodeType = fromNode.GetType();
        var properties = nodeType.GetProperties();

        foreach (var property in properties)
        {
            var relationshipAttribute = property.GetCustomAttributes()
                .FirstOrDefault(attr => attr.GetType().IsGenericType && attr.GetType()
                    .GetGenericTypeDefinition() == typeof(NodeRelationshipAttribute<>));

            if (relationshipAttribute == null)
                continue;
            var relatedNodeType = relationshipAttribute.GetType().GetGenericArguments()[0];
            var relatedNodeTypeName = relatedNodeType.Name;
            var relationshipName = relationshipAttribute.GetType().GetProperty("RelationshipName")?.GetValue(relationshipAttribute)?.ToString()?.ToUpper();
            // seedEdgeType will only be not null when the edge has custom properties
            var seedEdgeType = relationshipAttribute.GetType().GetProperty("SeedEdgeType")?
                .GetValue(relationshipAttribute) as Type;

            if (string.IsNullOrWhiteSpace(relationshipName))
            {
                _logger.LogError("RelationshipName is null or empty {NodeType}.", nodeType.Name);
                return false;
            }
            if (string.IsNullOrWhiteSpace(relatedNodeTypeName))
            {
                _logger.LogError("relatedNodeType is null or empty {NodeType}.", nodeType.Name);
                return false;
            }

            var value = property.GetValue(fromNode);
            if (value is not IEnumerable<string> relatedNodeIds)
                continue;

            // edges that have custom properties
            List<CustomEdge> edgeSeeds = [];
            if (seedEdgeType != null)
            {
                // Try to load edge data from DataSourceService
                edgeSeeds = _dataSourceService.GetSourceEdgesFor<CustomEdge>(seedEdgeType.Name).ToList();
                if (!edgeSeeds.Any())
                {
                    _logger.LogWarning("EdgeSeed type {seedEdgeType} specified but no edge data found in _dataSourceService.", seedEdgeType.Name);
                }
            }

            // Simple relationships (string IDs)
            foreach (var toNodeKey in relatedNodeIds)
            {
                // stock standard relationship with no properties on the edge
                if (seedEdgeType == null)
                {
                    await ExecuteUpsertRelationshipsAsync(fromNode,
                        session,
                        relatedNodeType,
                        relationshipName,
                        toNodeKey,
                        relatedNodeTypeName, [], string.Empty);
                    continue;
                }

                // get the edgeSeeds for this node
                var nodeEdgeSeeds = edgeSeeds.FindAll(seed =>
                    seed.GetFromId() == fromNode.GetPrimaryKeyValue()
                    && seed.GetToId() == toNodeKey);

                if (!nodeEdgeSeeds.Any())
                    continue;

                // should only be one, log if more than one
                if (nodeEdgeSeeds.Count > 1)
                {
                    _logger.LogWarning("Multiple ({count}) edge seeds found for {fromNode}-{toNode} on relationship {relationship}. Using first.",
                        nodeEdgeSeeds.Count, fromNode.GetPrimaryKeyValue(), toNodeKey, relationshipName);
                }

                var matchingEdge = nodeEdgeSeeds.First();

                var edgeParameters = new Dictionary<string, object?>();
                // create setClause and parameters for edge properties
                var edgeType = matchingEdge.GetType();
                List<string> setClauses = [];
                foreach (var prop in edgeType.GetProperties())
                {
                    if (prop.Name.Equals("FromId") || prop.Name.Equals("ToId"))
                        continue; // skip these
                    if (Attribute.IsDefined(prop, typeof(EdgePropertyIgnoreAttribute)))
                        continue;
                    var propValue = prop.GetValue(matchingEdge);
                    edgeParameters[prop.Name] = propValue;
                    setClauses.Add($"rel.{prop.Name} = ${prop.Name}");
                }

                var setClause = setClauses.Count > 0
                    ? "SET " + string.Join(", ", setClauses)
                    : string.Empty;

                await ExecuteUpsertRelationshipsAsync(fromNode,
                    session,
                    relatedNodeType,
                    relationshipName,
                    toNodeKey,
                    relatedNodeTypeName, edgeParameters, setClause);
            }
        }
        return true;
    }

    private async Task ExecuteUpsertRelationshipsAsync<T>(T fromNode, IAsyncSession session, Type relatedNodeType, string relationshipName,
        string toNodeKey, string relatedNodeTypeName, Dictionary<string, object?> parameters, string setClause = "") where T : GraphNode
    {
        // Determine PK name for the related type (fallback to "Id" if type cannot be constructed)
        var pkName = "Id";
        try
        {
            if (Activator.CreateInstance(relatedNodeType) is GraphNode tempInstance)
                pkName = tempInstance.GetPrimaryKeyName();
        }
        catch { /* ignore and use default */ }

        _logger.LogInformation("(:{node} {from})-[{relationship}]->{to}", fromNode.LabelName, fromNode.GetPrimaryKeyValue(), relationshipName, toNodeKey);

        parameters.Add("fromKey", fromNode.GetPrimaryKeyValue());
        parameters.Add("toKey", toNodeKey);

        var query =
            $$"""
              MATCH (from:{{fromNode.LabelName}} {{{fromNode.GetPrimaryKeyName()}}: $fromKey})
              MATCH (to:{{relatedNodeTypeName}} {{{pkName}}: $toKey})
              MERGE (from)-[rel:{{relationshipName}}]->(to)
              {{setClause}}
              """;
        await ExecuteWriteQuery(session, query, parameters);
    }

    #endregion

    #region LoadNodesViaPathNoEdges

    /// <summary>
    /// Traverses from a source node to related nodes via specified relationship path (variable-length pattern matching).
    /// Returns only the target nodes WITHOUT any edge data populated. For nodes WITH edges, use <see cref="LoadRelatedAsync{TSource,TRelated}"/> instead.
    /// </summary>
    /// <remarks>
    /// Cypher pattern generated:
    /// MATCH (s:SourceLabel { pk: $id })-[:REL1|REL2*min..max]-&gt;(t:TargetLabel)
    /// RETURN DISTINCT t AS node
    /// Validation enforces simple safe relationship tokens (alphanumeric &amp; underscore). Relationship names are
    /// upper‑cased to align with existing conventions (see ToGraphRelationShipCasing). Designed for read paths only.
    /// </remarks>
    public async Task<IReadOnlyList<TRelated>> LoadNodesViaPathNoEdgesAsync<TSource, TRelated>(string sourceId, string relationshipTypes, int minHops = 1, int maxHops = 4, IAsyncTransaction? tx = null,
        CancellationToken ct = default)
        where TSource : GraphNode, new()
        where TRelated : GraphNode, new()
    {
        ct.ThrowIfCancellationRequested();
        if (string.IsNullOrWhiteSpace(sourceId)) throw new ArgumentException("sourceId required", nameof(sourceId));
        if (string.IsNullOrWhiteSpace(relationshipTypes)) throw new ArgumentException("relationshipTypes required", nameof(relationshipTypes));
        if (minHops < 0) throw new ArgumentOutOfRangeException(nameof(minHops), "minHops cannot be negative");
        if (maxHops < minHops) throw new ArgumentOutOfRangeException(nameof(maxHops), "maxHops must be >= minHops");
        if (maxHops > 10) throw new ArgumentOutOfRangeException(nameof(maxHops), "maxHops > 10 likely indicates an inefficient query");

        var sourceTemp = new TSource();
        var sourceLabel = sourceTemp.LabelName;
        var sourcePk = sourceTemp.GetPrimaryKeyName();
        var targetLabel = typeof(TRelated).Name.ToPascalCase();

        var relTokens = relationshipTypes.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (relTokens.Length == 0) throw new ArgumentException("No valid relationship tokens provided", nameof(relationshipTypes));
        foreach (var r in relTokens)
        {
            if (!_labelValidationRegex.IsMatch(r))
                throw new ArgumentException($"Invalid relationship token '{r}'. Only A-Z, a-z, 0-9 and '_' allowed.", nameof(relationshipTypes));
        }

        var relPattern = string.Join('|', relTokens.Select(t => t.ToGraphRelationShipCasing()));
        var query = $$"""
                      MATCH (s:{{sourceLabel}} { {{sourcePk}}: $id })
                        -[:{{relPattern}}*{{minHops}}..{{maxHops}}]->
                        (t:{{targetLabel}})
                      RETURN DISTINCT t AS node
                      """;
        var parameters = new Dictionary<string, object> { { "id", sourceId } };

        return await ExecuteReadNodeQueryAsync<TRelated>(query, parameters, "node", tx, ct);
    }

    /// <summary>
    /// Traverses from a source node and returns only the distinct IDs of related nodes (no full node hydration, no edges).
    /// Lightweight variant useful for relationship/cascade operations where you only need IDs.
    /// Supports traversing outgoing, incoming or undirected (both) relationships.
    /// </summary>
    public async Task<IReadOnlyList<string>> LoadNodeIdsViaPathNoEdgesAsync<TRelated>(GraphNode fromNode, string relationshipTypes, int minHops = 1, int maxHops = 4,
        EdgeDirection direction = EdgeDirection.Outgoing, IAsyncTransaction? tx = null, CancellationToken ct = default)
        where TRelated : GraphNode, new()
    {
        ct.ThrowIfCancellationRequested();
        if (fromNode == null) throw new ArgumentException("from node required", "fromNode");
        if (string.IsNullOrWhiteSpace(relationshipTypes)) throw new ArgumentException("relationshipTypes required", nameof(relationshipTypes));
        if (minHops < 0) throw new ArgumentOutOfRangeException(nameof(minHops), "minHops cannot be negative");
        if (maxHops < minHops) throw new ArgumentOutOfRangeException(nameof(maxHops), "maxHops must be >= minHops");
        if (maxHops > 10) throw new ArgumentOutOfRangeException(nameof(maxHops), "maxHops > 10 likely indicates an inefficient query");

        var fromPkValue = fromNode.GetPrimaryKeyValue();
        var fromPkName = fromNode.GetPrimaryKeyName();

        var targetTemp = new TRelated();
        var targetLabel = targetTemp.LabelName; // use LabelName to honor overrides
        var targetPk = targetTemp.GetPrimaryKeyName();

        var relTokens = relationshipTypes.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (relTokens.Length == 0) throw new ArgumentException("No valid relationship tokens provided", nameof(relationshipTypes));
        foreach (var r in relTokens)
        {
            if (!_labelValidationRegex.IsMatch(r))
                throw new ArgumentException($"Invalid relationship token '{r}'. Only A-Z, a-z, 0-9 and '_' allowed.", nameof(relationshipTypes));
        }

        var relPattern = string.Join('|', relTokens.Select(t => t.ToGraphRelationShipCasing()));

        var dirPattern = direction switch
        {
            EdgeDirection.Outgoing => $"-[:{relPattern}*{minHops}..{maxHops}]->",
            EdgeDirection.Incoming => $"<-[:{relPattern}*{minHops}..{maxHops}]-",
            EdgeDirection.Both => $"-[:{relPattern}*{minHops}..{maxHops}]-",
            _ => throw new ArgumentException($"Invalid direction {direction}")
        };

        var query = $$"""
                      MATCH (s:{{fromNode.LabelName}} { {{fromPkName}}: $fromPkValue }){{dirPattern}}(t:{{targetLabel}})
                      RETURN DISTINCT t.{{targetPk}} AS id
                      """; // note using id alias for simplicity - this doesn't mean the property has to be 'id'

        var parameters = new Dictionary<string, object>
        {
            { "fromPkValue", fromPkValue }
        };

        async Task<IReadOnlyList<string>> ExecAsync(IAsyncQueryRunner runner)
        {
            try
            {
                var cursor = await runner.RunAsync(query, parameters);
                var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                while (await cursor.FetchAsync())
                {
                    if (!cursor.Current.Keys.Contains("id")) continue;
                    var val = cursor.Current["id"].As<string?>();
                    if (!string.IsNullOrWhiteSpace(val)) set.Add(val!);
                }

                return set.ToList();
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "LoadNodeIdsViaPathNoEdgesAsync failure. QueryLength={QueryLength}", query.Length);
                throw new RepositoryException("Failed executing related node id list read.", query, parameters.Keys, ex);
            }
        }

        if (tx != null)
        {
            return await ExecAsync(tx);
        }

        await using var session = StartSession();
        return await session.ExecuteReadAsync(async rtx => await ExecAsync(rtx));
    }

    #endregion

    #region CreateRelationships (aliases for UpsertRelationships)

    /// <summary>
    /// Creates relationships for a single node using attribute-defined relationships.
    /// Alias for UpsertRelationshipsAsync for backward compatibility.
    /// </summary>
    public async Task<bool> CreateRelationshipsAsync<T>(T node) where T : GraphNode
    {
        return await UpsertRelationshipsAsync(node);
    }

    /// <summary>
    /// Creates relationships for a single node using the provided session.
    /// Alias for UpsertRelationshipsAsync for backward compatibility.
    /// </summary>
    public async Task<bool> CreateRelationshipsAsync<T>(T node, IAsyncSession session) where T : GraphNode
    {
        return await UpsertRelationshipsAsync(node, session);
    }

    /// <summary>
    /// Creates relationships for a collection of nodes using attribute-defined relationships.
    /// Alias for UpsertRelationshipsAsync for backward compatibility.
    /// </summary>
    public async Task<bool> CreateRelationshipsAsync<T>(IEnumerable<T> nodes) where T : GraphNode
    {
        return await UpsertRelationshipsAsync(nodes);
    }

    #endregion

    #region LoadRelatedAsync (with edges)

    /// <summary>
    /// Loads related nodes of type <typeparamref name="TRelated"/> reachable from a source node via specified relationship(s),
    /// AND automatically populates all outgoing edge id lists and optional edge objects (like LoadAllAsync does).
    /// This eliminates the need for custom ExecuteReadListAsync queries when you want related nodes WITH their edges loaded.
    /// </summary>
    /// <remarks>
    /// Common use case: Load all Role nodes connected to a Team via FOR_TEAM relationship, with all Role edges populated.
    /// Previously required: ExecuteReadListAsync("MATCH (role:Role)-[:FOR_TEAM]->(team:Team {id: $teamId}) RETURN role", ...)
    /// Now use: LoadRelatedAsync&lt;Team, Role&gt;(teamId, "FOR_TEAM", minHops: 1, maxHops: 1, direction: EdgeDirection.Incoming)
    /// 
    /// Uses the same BuildLoadQuery helper that powers LoadAsync/LoadAllAsync to ensure consistent edge loading behavior.
    /// </remarks>
    public async Task<IReadOnlyList<TRelated>> LoadRelatedAsync<TSource, TRelated>(
        string sourceId,
        string relationshipTypes,
        int minHops = 1,
        int maxHops = 1,
        bool includeEdgeObjects = false,
        IEnumerable<string>? includeEdges = null,
        EdgeDirection direction = EdgeDirection.Outgoing,
        IAsyncTransaction? tx = null,
        CancellationToken ct = default)
        where TSource : GraphNode, new()
        where TRelated : GraphNode, new()
    {
        ct.ThrowIfCancellationRequested();
        if (string.IsNullOrWhiteSpace(sourceId)) throw new ArgumentException("sourceId required", nameof(sourceId));
        if (string.IsNullOrWhiteSpace(relationshipTypes)) throw new ArgumentException("relationshipTypes required", nameof(relationshipTypes));
        if (minHops < 0) throw new ArgumentOutOfRangeException(nameof(minHops), "minHops cannot be negative");
        if (maxHops < minHops) throw new ArgumentOutOfRangeException(nameof(maxHops), "maxHops must be >= minHops");
        if (maxHops > 10) throw new ArgumentOutOfRangeException(nameof(maxHops), "maxHops > 10 likely indicates an inefficient query");

        var sourceTemp = new TSource();
        var sourceLabel = sourceTemp.LabelName;
        var sourcePk = sourceTemp.GetPrimaryKeyName();

        var targetTemp = new TRelated();
        var targetLabel = targetTemp.LabelName;
        var targetPk = targetTemp.GetPrimaryKeyName();

        // Validate and normalize relationship tokens
        var relTokens = relationshipTypes.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (relTokens.Length == 0) throw new ArgumentException("No valid relationship tokens provided", nameof(relationshipTypes));
        foreach (var r in relTokens)
        {
            if (!_labelValidationRegex.IsMatch(r))
                throw new ArgumentException($"Invalid relationship token '{r}'. Only A-Z, a-z, 0-9 and '_' allowed.", nameof(relationshipTypes));
        }

        var relPattern = string.Join('|', relTokens.Select(t => t.ToGraphRelationShipCasing()));

        // Build direction pattern
        var dirPattern = direction switch
        {
            EdgeDirection.Outgoing => $"-[:{relPattern}*{minHops}..{maxHops}]->",
            EdgeDirection.Incoming => $"<-[:{relPattern}*{minHops}..{maxHops}]-",
            EdgeDirection.Both => $"-[:{relPattern}*{minHops}..{maxHops}]-",
            _ => throw new ArgumentException($"Invalid direction {direction}")
        };

        // Get relationship metadata for TRelated (this is what enables edge loading)
        var relationships = GetRelationshipMetadata(typeof(TRelated));
        var includeSet = includeEdges != null ? new HashSet<string>(includeEdges, StringComparer.OrdinalIgnoreCase) : null;

        // Build the traversal + edge loading query
        // Start with traversal to find related nodes
        var sb = new System.Text.StringBuilder();
        sb.Append($"MATCH (s:{sourceLabel} {{ {sourcePk}: $sourceId }}){dirPattern}(n:{targetLabel})\n");

        // Now add the edge loading logic (same as LoadAllAsync pattern)
        if (relationships.Count > 0)
        {
            for (var i = 0; i < relationships.Count; i++)
            {
                var r = relationships[i];
                sb.Append($"OPTIONAL MATCH (n)-[:{r.edgeName}]->(relNode{i}:{r.TargetLabel})\n");
                sb.Append($"WITH s, n, collect(DISTINCT relNode{i}.{r.TargetPrimaryKey}) AS {r.Alias}");
                if (i > 0)
                {
                    var carry = string.Join(", ", relationships.Take(i).Select(m => m.Alias));
                    sb.Append(", ").Append(carry);
                }
                sb.Append('\n');
            }

            sb.Append("RETURN DISTINCT n");
            foreach (var r in relationships)
                sb.Append($", {r.Alias}");

            // Add edge object pattern comprehensions if requested
            if (includeEdgeObjects)
            {
                for (var i = 0; i < relationships.Count; i++)
                {
                    var r = relationships[i];
                    if (r.EdgeObjectType == null) continue;

                    // Apply filter if specified
                    if (includeSet != null && includeSet.Count > 0)
                    {
                        if (!includeSet.Contains(r.Property.Name, StringComparer.OrdinalIgnoreCase)
                            && !includeSet.Contains(r.edgeName, StringComparer.OrdinalIgnoreCase)
                            && !includeSet.Contains(r.TargetLabel, StringComparer.OrdinalIgnoreCase))
                        {
                            continue; // skip this edge type
                        }
                    }

                    var relVar = $"rel{i}";
                    var toVar = $"to{i}";
                    var srcIdKey = $"{targetLabel}Id";
                    var tgtIdKey = $"{r.TargetLabel}Id";

                    sb.Append($", [ (n)-[{relVar}:{r.edgeName}]->({toVar}:{r.TargetLabel}) | ");
                    sb.Append($"{relVar} {{ .*");
                    sb.Append($", FromId: n.{targetPk}, ToId: {toVar}.{r.TargetPrimaryKey}");
                    sb.Append($", {srcIdKey}: n.{targetPk}, {tgtIdKey}: {toVar}.{r.TargetPrimaryKey}");
                    sb.Append(" } ] AS ").Append(r.ObjAlias);
                }
            }
        }
        else
        {
            // No edges defined on TRelated, just return the nodes
            sb.Append("RETURN DISTINCT n");
        }

        var query = sb.ToString();
        var parameters = new Dictionary<string, object> { { "sourceId", sourceId } };

        // Execute the query
        async Task<IReadOnlyList<TRelated>> ExecAsync(IAsyncQueryRunner runner)
        {
            try
            {
                var cursor = await runner.RunAsync(query, parameters);
                var results = new List<TRelated>();

                while (await cursor.FetchAsync())
                {
                    var record = cursor.Current;
                    if (!record.Keys.Contains("n")) continue;

                    var node = record["n"].As<INode>();
                    var entity = MapNodeToObject<TRelated>(node);

                    // Map relationship id lists
                    MapRelationshipLists(record, entity, relationships);

                    // Map edge objects if requested
                    if (includeEdgeObjects)
                        MapEdgeObjects(record, entity, relationships);

                    results.Add(entity);
                }

                return results;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "LoadRelatedAsync failure. QueryLength={QueryLength}", query.Length);
                throw new RepositoryException("Failed executing related nodes load with edges.", query, parameters.Keys, ex);
            }
        }

        if (tx != null)
        {
            return await ExecAsync(tx);
        }

        await using var session = StartSession();
        return await session.ExecuteReadAsync(async rtx => await ExecAsync(rtx));
    }

    #endregion

    #region Alias Methods for Backward Compatibility

    /// <summary>
    /// Loads related nodes reachable from a source node via specified relationships.
    /// Alias for LoadRelatedAsync with simplified parameters.
    /// </summary>
    public async Task<IReadOnlyList<TRelated>> LoadRelatedNodesAsync<TSource, TRelated>(
        string sourceId,
        string relationshipTypes,
        int minHops = 1,
        int maxHops = 1,
        IAsyncTransaction? tx = null,
        CancellationToken ct = default)
        where TSource : GraphNode, new()
        where TRelated : GraphNode, new()
    {
        return await LoadRelatedAsync<TSource, TRelated>(
            sourceId,
            relationshipTypes,
            minHops,
            maxHops,
            includeEdgeObjects: false,
            includeEdges: null,
            direction: EdgeDirection.Outgoing,
            tx,
            ct);
    }

    /// <summary>
    /// Returns only the distinct IDs of related nodes (no full node hydration).
    /// Alias for LoadNodeIdsViaPathNoEdgesAsync.
    /// </summary>
    public async Task<IReadOnlyList<string>> LoadRelatedNodeIdsAsync<TRelated>(
        GraphNode fromNode,
        string relationshipTypes,
        int minHops = 1,
        int maxHops = 1,
        EdgeDirection direction = EdgeDirection.Outgoing,
        IAsyncTransaction? tx = null,
        CancellationToken ct = default)
        where TRelated : GraphNode, new()
    {
        return await LoadNodeIdsViaPathNoEdgesAsync<TRelated>(
            fromNode,
            relationshipTypes,
            minHops,
            maxHops,
            direction,
            tx,
            ct);
    }

    #endregion
}
