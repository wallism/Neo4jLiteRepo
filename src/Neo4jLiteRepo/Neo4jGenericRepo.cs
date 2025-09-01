using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4j.Driver.Internal.Result;
using Neo4jLiteRepo.Attributes;
using Neo4jLiteRepo.Exceptions;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.Models;
using Neo4jLiteRepo.NodeServices;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Text.RegularExpressions;

namespace Neo4jLiteRepo
{
    public interface INeo4jGenericRepo : IDisposable
    {
        /// <summary>
        /// Enforces unique constraints on the specified node services in the Neo4j database.
        /// </summary>
        Task<bool> EnforceUniqueConstraints(IEnumerable<INodeService> nodeServices);

        /// <summary>
        /// Creates a vector index for embeddings on the specified labels with the given dimensions.
        /// </summary>
        Task<bool> CreateVectorIndexForEmbeddings(IList<string>? labelNames = null, int dimensions = 3072);


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
        /// Creates relationships for a collection of nodes in the Neo4j database.
        /// </summary>
        Task<bool> CreateRelationshipsAsync<T>(IEnumerable<T> fromNodes) where T : GraphNode;

        /// <summary>
        /// Creates relationships for a single node in the Neo4j database.
        /// </summary>
        Task<bool> CreateRelationshipsAsync<T>(T nodes) where T : GraphNode;

        /// <summary>
        /// Creates relationships for a node using the provided session.
        /// </summary>
        Task<bool> CreateRelationshipsAsync<T>(T nodes, IAsyncSession session) where T : GraphNode;

        /// <summary>
        /// Retrieves all nodes and their relationships
        /// </summary>
        Task<NodeRelationshipsResponse> GetAllNodesAndRelationshipsAsync();

        /// <summary>
        /// Retrieves all nodes and their relationships using the provided session.
        /// </summary>
        Task<NodeRelationshipsResponse> GetAllNodesAndRelationshipsAsync(IAsyncSession session);


        /// <summary>
        /// Executes a read query and returns a list of objects of type T.
        /// </summary>
        Task<IEnumerable<T>> ExecuteReadListAsync<T>(string query, string returnObjectKey, IDictionary<string, object>? parameters = null)
            where T : class, new();

        /// <summary>
        /// Streams results without materializing entire result set in memory. Caller should enumerate promptly; the underlying session is disposed when enumeration completes.
        /// </summary>
        IAsyncEnumerable<T> ExecuteReadStreamAsync<T>(string query, string returnObjectKey, IDictionary<string, object>? parameters = null)
            where T : class, new();

        /// <summary>
        /// Executes a read query and returns a list of strings from the result.
        /// </summary>
        Task<IEnumerable<string>> ExecuteReadListStringsAsync(string query, string returnObjectKey, IDictionary<string, object>? parameters = null);

        /// <summary>
        /// Executes a read query and returns a scalar value of type T.
        /// </summary>
        Task<T> ExecuteReadScalarAsync<T>(string query, IDictionary<string, object>? parameters = null);

        /// <summary>
        /// Executes a vector similarity search query to find relevant content chunks
        /// </summary>
        /// <param name="questionEmbedding">The embedding vector of the question</param>
        /// <param name="topK">Number of most relevant chunks to return</param>
        /// <param name="includeContext">Whether to include related chunks and parent context</param>
        /// <param name="similarityThreshold">Minimum cosine similarity threshold (0-1) for matching content</param>
        /// <returns>A list of strings containing the content and article information</returns>
        Task<List<string>> ExecuteVectorSimilaritySearchAsync(
            float[] questionEmbedding,
            int topK = 5,
            bool includeContext = true,
            double similarityThreshold = 0.6);



        /// <summary>
        /// Starts and returns a new asynchronous Neo4j session.
        /// </summary>
        IAsyncSession StartSession();

        /// <summary>
        /// Merges a relationship between two nodes in the Neo4j database.
        /// </summary>
        Task MergeRelationshipAsync(GraphNode fromNode, string rel, GraphNode toNode, CancellationToken ct = default);

        /// <summary>
        /// Merges a relationship between two nodes using the provided transaction.
        /// </summary>
        Task MergeRelationshipAsync(GraphNode fromNode, string rel, GraphNode toNode, IAsyncTransaction tx, CancellationToken ct = default);

        /// <summary>
        /// delete node - DETACH DELETE so all edges are also removed.
        /// </summary>
        Task<IResultSummary> DetachDeleteAsync<T>(T node, CancellationToken ct = default)
            where T : GraphNode, new();
        /// <summary>
        /// delete node - DETACH DELETE so all edges are also removed.
        /// </summary>
        Task<IResultSummary> DetachDeleteAsync<T>(T node, IAsyncTransaction tx, CancellationToken ct = default)
            where T : GraphNode, new();

        Task<IResultSummary> DetachDeleteAsync<T>(string pkValue, IAsyncTransaction tx, CancellationToken ct = default) 
            where T : GraphNode, new();
        /// <summary>
        /// delete node - DETACH DELETE so all edges are also removed.
        /// </summary>
        Task<IResultSummary> DetachDeleteAsync<T>(string pkValue, CancellationToken ct = default) 
            where T : GraphNode, new();



        Task<IResultSummary> DetachDeleteManyAsync<T>(List<T> nodes, CancellationToken ct = default)
            where T : GraphNode, new();
        Task<IResultSummary> DetachDeleteManyAsync<T>(List<T> nodes, IAsyncTransaction tx, CancellationToken ct = default)
            where T : GraphNode, new();
        Task<IResultSummary> DetachDeleteManyAsync<T>(List<string> ids, CancellationToken ct = default)
            where T : GraphNode, new();
        Task<IResultSummary> DetachDeleteManyAsync<T>(List<string> ids, IAsyncTransaction tx, CancellationToken ct = default)
            where T : GraphNode, new();

        /// <summary>
        /// deletes (DETACH DELETE) nodes by id creating its own session/transaction.
        /// </summary>
        /// <param name="label">Node label</param>
        /// <param name="ids">Primary key values (id property)</param>
        /// <param name="ct">Cancellation token</param>
        Task DetachDeleteNodesByIdsAsync(string label, IEnumerable<string> ids, CancellationToken ct = default);

        /// <summary>
        /// Deletes (DETACH DELETE) nodes by id using an existing session (caller controls session lifetime for batching).
        /// Internally wraps the operation in a transaction for atomicity.
        /// </summary>
        /// <param name="label">Node label</param>
        /// <param name="ids">Primary key values (id property)</param>
        /// <param name="session">Existing async session</param>
        /// <param name="ct">Cancellation token</param>
        Task DetachDeleteNodesByIdsAsync(string label, IEnumerable<string> ids, IAsyncSession session, CancellationToken ct = default);

        /// <summary>
        /// Deletes nodes by id using the provided transaction, detaching all relationships.
        /// </summary>
        Task DetachDeleteNodesByIdsAsync(string label, IEnumerable<string> ids, IAsyncTransaction tx, CancellationToken ct = default);

        // Delete relationship signatures mirror MergeRelationshipAsync (session-managed and tx-based),
        // but include a required direction parameter specific to deletion semantics.
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
        /// Removes orphan nodes (nodes with zero relationships) for label derived from <typeparamref name="T"/>.
        /// Runs in its own write transaction. Returns number of deleted nodes.
        /// Uses efficient batch deletion to avoid memory spikes and query limits.
        /// </summary>
        Task<int> RemoveOrphansAsync<T>(CancellationToken ct = default) where T : GraphNode, new();

        /// <summary>
        /// Removes orphan nodes (nodes with zero relationships) for label derived from <typeparamref name="T"/> using an existing transaction.
        /// Returns the number of deleted nodes (for this single pass – if new orphans appear later call again).
        /// Uses efficient batch deletion to avoid memory spikes and query limits.
        /// </summary>
        Task<int> RemoveOrphansAsync<T>(IAsyncTransaction tx, CancellationToken ct = default) where T : GraphNode, new();

        /// <summary>
        /// Removes orphan nodes (nodes with zero relationships) for label derived from <typeparamref name="T"/> using an existing session.
        /// Opens a write transaction internally. Returns number of deleted nodes.
        /// Uses efficient batch deletion to avoid memory spikes and query limits.
        /// </summary>
        Task<int> RemoveOrphansAsync<T>(IAsyncSession session, CancellationToken ct = default) where T : GraphNode, new();

        // Optional helpers to run custom Cypher for cascade deletes (domain-specific cascades should live outside generic repo)
        /// <summary>
        /// Checks if the specified content chunk has an embedding vector.
        /// </summary>
        Task<bool> ContentChunkHasEmbeddingAsync(string chunkId, IAsyncTransaction tx, CancellationToken ct = default);

        /// <summary>
        /// Updates the embedding vector and hash for the specified content chunk using the provided transaction.
        /// </summary>
        Task UpdateChunkEmbeddingAsync(string chunkId, float[] vector, string? hash, IAsyncTransaction tx, CancellationToken ct = default);

        /// <summary>
        /// Retrieves the text and hash for the specified content chunk using the provided transaction.
        /// </summary>
        Task<(string? Text, string? Hash)> GetChunkTextAndHashAsync(string chunkId, IAsyncTransaction tx, CancellationToken ct = default);

        /// <summary>
        /// Loads a single node of type <typeparamref name="T"/> by its primary key value (usually Id) and populates any outgoing relationship id lists.
        /// </summary>
        /// <typeparam name="T">Concrete type inheriting from <see cref="GraphNode"/></typeparam>
        /// <param name="id">Primary key value to match</param>
        /// <param name="ct">Cancellation token</param>
        /// <returns>The loaded node instance or null if not found.</returns>
        Task<T?> LoadAsync<T>(string id, CancellationToken ct = default) where T : GraphNode, new();

        /// <summary>
        /// Loads all nodes of type <typeparamref name="T"/> and populates any outgoing relationship id lists (List<string>) defined via <see cref="NodeRelationshipAttribute{T}"/>.
        /// </summary>
        /// <typeparam name="T">Concrete type inheriting from <see cref="GraphNode"/></typeparam>
        /// <param name="ct">Cancellation token</param>
        /// <returns>Collection (possibly empty) of loaded nodes.</returns>
        Task<IReadOnlyList<T>> LoadAllAsync<T>(CancellationToken ct = default) where T : GraphNode, new();

        Task<IEnumerable<T>> ExecuteReadListAsync<T>(string query,
            string returnObjectKey, IAsyncSession session, IDictionary<string, object>? parameters = null)
            where T : class, new();

        Task<T?> LoadAsync<T>(string id, bool includeEdgeObjects, IEnumerable<string>? includeEdges, CancellationToken ct = default)
            where T : GraphNode, new();

        /// <summary>
        /// Loads all nodes of a given type and populates outgoing relationship List&lt;string&gt; properties defined with <see cref="NodeRelationshipAttribute{T}"/>.
        /// Only related node primary key values are populated (not full node objects) to keep the load lightweight.
        /// Supports pagination via skip/take.
        /// </summary>
        /// <typeparam name="T">Concrete GraphNode type to load.</typeparam>
        /// <param name="skip">Number of records to skip (for pagination).</param>
        /// <param name="take">Maximum number of records to take (for pagination).</param>
        /// <param name="ct">Cancellation token.</param>
        Task<IReadOnlyList<T>> LoadAllAsync<T>(int skip, int take, CancellationToken ct = default) where T : GraphNode, new();

        Task<IReadOnlyList<T>> LoadAllAsync<T>(int skip, int take, bool includeEdgeObjects, IEnumerable<string>? includeEdges, CancellationToken ct = default)
            where T : GraphNode, new();
        /// <summary>
        /// Loads related nodes of type <typeparamref name="TRelated"/> reachable from a source node of type <typeparamref name="TSource"/>
        /// via any of the supplied relationship types within the specified hop range.
        /// </summary>
        /// <typeparam name="TSource">Source node label type (must derive from GraphNode).</typeparam>
        /// <typeparam name="TRelated">Target/related node label type to return (must derive from GraphNode).</typeparam>
        /// <param name="sourceId">Primary key value of the source node.</param>
        /// <param name="relationshipTypes">Pipe (|) delimited list of relationship type names (e.g. "REL_A|REL_B|REL_C").</param>
        /// <param name="minHops">Minimum traversal hops (inclusive). Usually 0 or 1.</param>
        /// <param name="maxHops">Maximum traversal hops (inclusive). Keep small (<=5) for performance.</param>
        /// <param name="tx">Optional existing transaction to participate in; when null a temporary session/read tx is created.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Distinct related nodes mapped to <typeparamref name="TRelated"/>.</returns>
        Task<IReadOnlyList<TRelated>> LoadRelatedNodesAsync<TSource, TRelated>(string sourceId, string relationshipTypes, int minHops = 1, int maxHops = 4, IAsyncTransaction? tx = null,
            CancellationToken ct = default)
            where TSource : GraphNode, new()
            where TRelated : GraphNode, new();

        /// <summary>
        /// Loads the distinct ids of related <typeparamref name="TRelated"/> nodes reachable from a source node of type <typeparamref name="TSource"/>
        /// via any of the supplied relationship types within the specified hop range. Unlike <see cref="LoadRelatedNodesAsync{TSource,TRelated}"/>
        /// this returns only primary key values (ids) without hydrating full node objects – useful for lightweight relationship / cascade operations.
        /// </summary>
        /// <typeparam name="TSource">Source node label type (must derive from <see cref="GraphNode"/>).</typeparam>
        /// <typeparam name="TRelated">Target/related node label type (must derive from <see cref="GraphNode"/>).</typeparam>
        /// <param name="sourceId">Primary key value of the source node.</param>
        /// <param name="relationshipTypes">Pipe (|) delimited list of relationship type names (e.g. "REL_A|REL_B").</param>
        /// <param name="minHops">Minimum traversal hops (inclusive).</param>
        /// <param name="maxHops">Maximum traversal hops (inclusive).</param>
        /// <param name="direction">Traversal direction relative to the source node (default Outgoing).</param>
        /// <param name="tx">Optional existing transaction to participate in; when null a temporary session/read tx is created.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Distinct related node id values.</returns>
        Task<IReadOnlyList<string>> LoadRelatedNodeIdsAsync<TRelated>(GraphNode fromNode, string relationshipTypes, int minHops = 1, int maxHops = 4,
            EdgeDirection direction = EdgeDirection.Outgoing, IAsyncTransaction? tx = null, CancellationToken ct = default)
            where TRelated : GraphNode, new();
    }

    
    public class Neo4jGenericRepo(
        ILogger<Neo4jGenericRepo> logger,
        IConfiguration config,
        IDriver neo4jDriver,
        IDataSourceService dataSourceService) : IAsyncDisposable, INeo4jGenericRepo
    {
        /// <inheritdoc/>
        public void Dispose()
        {
            neo4jDriver.Dispose();
        }

        protected string Now => DateTimeOffset.Now.ToLocalTime().ToString("O");

        /// <inheritdoc/>
        public IAsyncSession StartSession()
        {
            return neo4jDriver.AsyncSession();
        }

        /// <summary>
        /// Executes a write query using the provided session.
        /// </summary>
        private async Task<IResultSummary> ExecuteWriteQuery(IAsyncSession session, string query)
        {
            return await session.ExecuteWriteAsync(async tx => await ExecuteWriteQuery(tx, query));
        }

        /// <summary>
        /// Executes a write query using the provided query runner.
        /// </summary>
        private async Task<IResultSummary> ExecuteWriteQuery(IAsyncQueryRunner runner, string query)
        {
            try
            {
                var cursor = await runner.RunAsync(query, new { Now });
                return await cursor.ConsumeAsync();
            }
            catch (AuthenticationException authEx)
            {
                logger.LogError(authEx, "ExecuteWriteQuery auth error (runner).");
                throw;
            }
            catch (OperationCanceledException)
            {
                logger.LogWarning("[OperationCanceled] ExecuteWriteQuery auth error (runner).");
                throw;
            }
            catch (Exception ex)
            {
                // only write to console in case there are secrets in the query
                Console.WriteLine($"**** write query failed ****{query}");
                logger.LogError(ex, "ExecuteWriteQuery (runner) failure. QueryLength={QueryLength}", query.Length);
                throw new RepositoryException("Failed executing write query (runner).", query, ["Now"], ex);
            }
        }

        /// <summary>
        /// Executes a write query with parameters using the provided query runner.
        /// </summary>
        private async Task<IResultSummary> ExecuteWriteQuery(IAsyncQueryRunner runner, string query, object parameters)
        {
            try
            {
                // If caller supplies a dictionary, add Now directly
                object finalParams;
                if (parameters is IDictionary<string, object?> pdict)
                {
                    if (!pdict.ContainsKey("Now"))
                        pdict["Now"] = Now;
                    finalParams = parameters;
                }
                else
                {
                    // Merge in the common Now parameter if caller did not supply it.
                    var paramType = parameters.GetType();
                    var hasNow = paramType.GetProperties().Any(p => string.Equals(p.Name, "Now", StringComparison.OrdinalIgnoreCase));
                    finalParams = hasNow ? parameters : new { Now, Parameters = parameters };
                    if (!hasNow)
                    {
                        // Build an expando with existing props + Now (reflection copy)
                        var expando = new System.Dynamic.ExpandoObject();
                        var dict = (IDictionary<string, object?>)expando;
                        foreach (var p in paramType.GetProperties())
                            dict[p.Name] = p.GetValue(parameters);
                        dict["Now"] = Now;
                        finalParams = expando;
                    }
                }

                var cursor = await runner.RunAsync(query, finalParams);
                return await cursor.ConsumeAsync();
            }
            catch (AuthenticationException authEx)
            {
                logger.LogError(authEx, "ExecuteWriteQuery auth error (runner/param).");
                throw;
            }
            catch (OperationCanceledException)
            {
                logger.LogWarning("[OperationCanceled] ExecuteWriteQuery auth error (runner/param).");
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"**** write query failed ****{query}");
                logger.LogError(ex, "ExecuteWriteQuery (runner/param) failure. QueryLength={QueryLength}", query.Length);
                // Attempt to surface parameter property names (best-effort)
                var paramNames = parameters.GetType().GetProperties().Select(p => p.Name).ToArray();
                throw new RepositoryException("Failed executing write query (runner/param).", query, paramNames, ex);
            }
        }


        /// <summary>
        /// Deletes (DETACH DELETE - i.e. all relationships connected to the node will also be deleted)
        /// nodes of the specified label whose id property matches any of the provided ids.
        /// Uses batching + UNWIND for large collections to stay within query / memory limits. Assumes identity property 'id'.
        /// </summary>
        public async Task DetachDeleteNodesByIdsAsync(string label, IEnumerable<string> ids, IAsyncTransaction tx, CancellationToken ct = default)
        {
            if (tx == null) throw new ArgumentNullException(nameof(tx));
            if (string.IsNullOrWhiteSpace(label)) throw new ArgumentException("Label is required", nameof(label));
            if (!_labelValidationRegex.IsMatch(label))
                throw new ArgumentException($"Invalid label '{label}'. Only A-Z, a-z, 0-9 and '_' allowed.", nameof(label));

            if (!IsInDetachDeleteWhitelist(label))
            {
                logger.LogWarning("Label {Label} is not in the whitelist for DetachDeleteNodesByIdsAsync; skipping delete operation", label);
                return;
            }

            var idList = ids?.Where(s => !string.IsNullOrWhiteSpace(s))
                .Select(s => s.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList() ?? [];
            if (idList.Count == 0)
            {
                logger.LogInformation("DeleteNodesByIdsAsync called with 0 ids for label {Label}; nothing to do", label);
                return;
            }

            const int batchSize = 500; // consider making configurable
            var total = idList.Count;
            var processed = 0;
            var sw = System.Diagnostics.Stopwatch.StartNew();
            for (var i = 0; i < idList.Count; i += batchSize)
            {
                ct.ThrowIfCancellationRequested();
                var batch = idList.Skip(i).Take(batchSize).ToList();
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
                    logger.LogError(ex, "Failed deleting nodes batch {Start}-{End} of {Total} for label {Label}", i + 1, i + batch.Count, total, label);
                    throw;
                }

                processed += batch.Count;
            }

            sw.Stop();
            logger.LogInformation("DeleteNodesByIdsAsync deleted {Count} nodes for label {Label} in {ElapsedMs}ms (batches of {BatchSize})", processed, label, sw.ElapsedMilliseconds, batchSize);
        }

        private bool IsInDetachDeleteWhitelist(string label)
        {
            // Only allow certain labels to be deleted via this method to avoid accidental mass deletes.
            logger.LogWarning("ZERO labels defined on whitelist"); // todo: make configurable
            return false;
        }



        public async Task<IResultSummary> DetachDeleteAsync<T>(T node, CancellationToken ct = default)
            where T : GraphNode, new()
        {
            if (node == null) throw new ArgumentNullException(nameof(node));

            await using var session = neo4jDriver.AsyncSession();
            await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
            try
            {
                var result = await DetachDeleteAsync(node, tx, ct).ConfigureAwait(false);
                await tx.CommitAsync().ConfigureAwait(false);
                return result;
            }
            catch
            {
                try
                {
                    await tx.RollbackAsync().ConfigureAwait(false);
                }
                catch { /* ignore */ }
                throw;
            }
        }

        public async Task<IResultSummary> DetachDeleteAsync<T>(T node, IAsyncTransaction tx, CancellationToken ct = default)
            where T : GraphNode, new()
        {
            if (node == null) throw new ArgumentNullException(nameof(node));
            var pkValue = node.GetPrimaryKeyValue();
            return await DetachDeleteAsync<T>(pkValue, tx, ct);
        }


        public async Task<IResultSummary> DetachDeleteAsync<T>(string pkValue, CancellationToken ct = default)
            where T : GraphNode, new()
        {
            await using var session = neo4jDriver.AsyncSession();
            await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
            try
            {
                var result = await DetachDeleteAsync<T>(pkValue, tx, ct).ConfigureAwait(false);
                await tx.CommitAsync().ConfigureAwait(false);
                return result;
            }
            catch
            {
                try
                {
                    await tx.RollbackAsync().ConfigureAwait(false);
                }
                catch { /* ignore */ }
                throw;
            }
        }

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
            var parameters = new Dictionary<string, object> {{ "pkValue", pkValue }};
            try
            {
                return await tx.RunWriteAsync(cypher, parameters);
            }
            catch (Exception ex) // catch to log
            {
                logger.LogError(ex, "running cypher: {query}", cypher);
                throw;
            }
        }


        public async Task<IResultSummary> DetachDeleteManyAsync<T>(List<T> nodes, CancellationToken ct = default) 
            where T : GraphNode, new()
        {
            await using var session = neo4jDriver.AsyncSession();
            await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
            try
            {
                var result = await DetachDeleteManyAsync(nodes, tx, ct).ConfigureAwait(false);
                await tx.CommitAsync().ConfigureAwait(false);
                return result;
            }
            catch
            {
                try
                {
                    await tx.RollbackAsync().ConfigureAwait(false);
                }
                catch { /* ignore */ }
                throw;
            }
        }

        public async Task<IResultSummary> DetachDeleteManyAsync<T>(List<T> nodes, IAsyncTransaction tx, CancellationToken ct = default)
            where T : GraphNode, new()
        {
            if (nodes == null) throw new ArgumentNullException(nameof(nodes));
            var ids = nodes.Select(n => n.GetPrimaryKeyValue()).ToList();
            return await DetachDeleteManyAsync<T>(ids, tx, ct);
        }

        public async Task<IResultSummary> DetachDeleteManyAsync<T>(List<string> ids, CancellationToken ct = default) 
            where T : GraphNode, new()
        {
            await using var session = neo4jDriver.AsyncSession();
            await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
            try
            {
                var result = await DetachDeleteManyAsync<T>(ids, tx, ct).ConfigureAwait(false);
                await tx.CommitAsync().ConfigureAwait(false);
                return result;
            }
            catch
            {
                try
                {
                    await tx.RollbackAsync().ConfigureAwait(false);
                }
                catch { /* ignore */ }
                throw;
            }
        }

        public async Task<IResultSummary> DetachDeleteManyAsync<T>(List<string> ids, IAsyncTransaction tx, CancellationToken ct = default) 
            where T : GraphNode, new()
        {
            ct.ThrowIfCancellationRequested();
            if (tx == null) throw new ArgumentNullException(nameof(tx));
            if (ids.Count == 0) throw new ArgumentNullException(nameof(ids));

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

        /// <summary>
        /// detach delete nodes by id.creates its own session & transaction.
        /// Mirrors the pattern used by UpsertNodes convenience overloads.
        /// </summary>
        /// <param name="label">Node label</param>
        /// <param name="ids">Primary key values (id property)</param>
        /// <param name="ct">Cancellation token</param>
        public async Task DetachDeleteNodesByIdsAsync(string label, IEnumerable<string> ids, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await using var session = neo4jDriver.AsyncSession();
            await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
            try
            {
                await DetachDeleteNodesByIdsAsync(label, ids, tx, ct).ConfigureAwait(false);
                await tx.CommitAsync().ConfigureAwait(false);
            }
            catch
            {
                try
                {
                    await tx.RollbackAsync().ConfigureAwait(false);
                }
                catch
                {
                    /* ignore */
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
            if (session == null) throw new ArgumentNullException(nameof(session));
            ct.ThrowIfCancellationRequested();
            await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
            try
            {
                await DetachDeleteNodesByIdsAsync(label, ids, tx, ct).ConfigureAwait(false);
                await tx.CommitAsync().ConfigureAwait(false);
            }
            catch
            {
                try
                {
                    await tx.RollbackAsync().ConfigureAwait(false);
                }
                catch
                {
                    /* ignore */
                }

                throw;
            }
        }


        /// <summary>
        /// Merges (creates if missing) a relationship of type <paramref name="rel"/> from one node to another.
        /// </summary>
        public async Task MergeRelationshipAsync(GraphNode fromNode, string rel, GraphNode toNode, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await using var session = neo4jDriver.AsyncSession();
            await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
            try
            {
                // Use new refactored method below
                await MergeRelationshipAsync(fromNode, rel, toNode, tx, ct);
                await tx.CommitAsync().ConfigureAwait(false);
            }
            catch
            {
                try
                {
                    await tx.RollbackAsync().ConfigureAwait(false);
                }
                catch
                {
                    /* ignore */
                }

                throw;
            }
        }

        /// <summary>
        /// Merges (creates if missing) a relationship of type <paramref name="rel"/> from one node to another.
        /// </summary>
        public async Task MergeRelationshipAsync(GraphNode fromNode, string rel, GraphNode toNode, IAsyncTransaction tx, CancellationToken ct = default)
        {
            //await MergeRelationshipAsync(fromNode.GetType().Name, fromNode.Id, rel, toNode.GetType().Name, toNode.Id, tx, ct);

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
                logger.LogInformation("MERGE {FromLabel}:{FromPkValue}-[{Rel}]->{ToLabel}:{ToPkValue}", fromNode.LabelName, fromPkValue, rel, toNode.LabelName, toPkValue);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Failed merging relationship {Rel} {FromLabel}:{FromPkValue}->{ToLabel}:{ToPkValue}", rel, fromNode.LabelName, fromPkValue, toNode.LabelName, toPkValue);
                throw;
            }
        }




        /// <summary>
        /// Deletes a single relationship of the specified type between two nodes (specify direction).
        /// Session-managed overload to mirror MergeRelationshipAsync.
        /// </summary>
        public async Task DeleteRelationshipAsync(GraphNode fromNode, string rel, GraphNode toNode, EdgeDirection direction, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await using var session = neo4jDriver.AsyncSession();
            await using var tx = await session.BeginTransactionAsync().ConfigureAwait(false);
            try
            {
                await DeleteRelationshipAsync(fromNode, rel, toNode, direction, tx, ct).ConfigureAwait(false);
                await tx.CommitAsync().ConfigureAwait(false);
            }
            catch
            {
                try { await tx.RollbackAsync().ConfigureAwait(false); }
                catch { /* ignore */ }
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
                logger.LogInformation("Deleted relationship {Rel} {FromLabel}:{FromPkValue} -> {ToLabel}:{ToPkValue}", rel, fromNode.LabelName, fromPkValue, toNode.LabelName, toPkValue);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Failed deleting relationship {Rel} {FromLabel}:{FromPkValue}->{ToLabel}:{ToPkValue}", rel, fromNode.LabelName, fromPkValue, toNode.LabelName, toPkValue);
                throw;
            }
        }

        /// <summary>
        /// Deletes multiple relationships (single edges) in batches. Each spec identifies a potential relationship between two nodes.
        /// Groups by (FromLabel, ToLabel, Rel, Direction) so labels & rel type can be inlined safely (identifiers cannot be parameterized in Cypher).
        /// </summary>
        /// <remarks>
        /// Similar validation & patterns as <see cref="DeleteRelationshipAsync"/> but optimized for bulk removal.
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
                logger.LogInformation("DeleteEdgesAsync called with 0 valid specs; nothing to do");
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
                if(specsInGroup.Count == 0)
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
                        logger.LogError(ex,
                            "Failed deleting relationship batch {Start}-{End} of {GroupCount} (TotalSpecs={Total}) {FromLabel}-{Rel}-{ToLabel} Direction={Direction}",
                            i + 1, i + batchPairs.Count, specsInGroup.Count, total, group.Key.FromNode.LabelName, group.Key.Rel, group.Key.ToNode.LabelName, group.Key.Direction);
                        throw;
                    }
                }
            }

            sw.Stop();
            logger.LogInformation("DeleteRelationshipsAsync deleted up to {Processed} relationship specs in {ElapsedMs}ms (batches of {BatchSize})", processed, sw.ElapsedMilliseconds, batchSize);
        }

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
                if(result.Counters.RelationshipsDeleted > 0)
                    logger.LogInformation("Deleted all ({count}) {Rel} relationships from {Label}:{Id}", result.Counters.RelationshipsDeleted, rel, fromNode.LabelName, fromPkValue);
                return result;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Failed deleting relationships {Rel} from {Label}:{Id}", rel, fromNode.LabelName, fromPkValue);
                throw;
            }
        }

        // Domain-specific cascade deletes (like Section subtree) intentionally moved out to service layer.

        public Task<bool> ContentChunkHasEmbeddingAsync(string chunkId, IAsyncTransaction tx, CancellationToken ct = default)
        {
            throw new NotImplementedException();
        }

        public Task UpdateChunkEmbeddingAsync(string chunkId, float[] vector, string? hash, IAsyncTransaction tx, CancellationToken ct = default)
        {
            throw new NotImplementedException();
        }

        public Task<(string? Text, string? Hash)> GetChunkTextAndHashAsync(string chunkId, IAsyncTransaction tx, CancellationToken ct = default)
        {
            throw new NotImplementedException();
        }

        // Validation helpers (keep internal to centralize any future pattern changes)
        private static void ValidateLabel(string value, string paramName)
        {
            if (string.IsNullOrWhiteSpace(value) || !_labelValidationRegex.IsMatch(value))
                throw new ArgumentException($"Invalid label '{value}'", paramName);
        }

        private static void ValidateRel(string value, string paramName)
        {
            // Reuse same regex constraints as labels for now (only alphanum + underscore)
            if (string.IsNullOrWhiteSpace(value) || !_labelValidationRegex.IsMatch(value))
                throw new ArgumentException($"Invalid relationship type '{value}'", paramName);
        }

        public async Task<IResultSummary> UpsertNode<T>(T node, CancellationToken ct = default) where T : GraphNode
        {
            await using var session = neo4jDriver.AsyncSession();
            return await UpsertNode(node, session, ct);
        }

        /// <summary>
        /// Upserts a node using an existing session. Caller is responsible for disposing the session.
        /// </summary>
        public async Task<IResultSummary> UpsertNode<T>(T node, IAsyncSession session, CancellationToken ct = default) where T : GraphNode
        {
            logger.LogInformation("({label}:{node})", node.LabelName, node.DisplayName);
            await using var tx = await session.BeginTransactionAsync();
            try
            {
                var result = await UpsertNode(node, tx, ct);
                await tx.CommitAsync();
                return result;
            }
            catch (Exception)
            {
                await tx.RollbackAsync();
                throw;
            }
        }

        public async Task<IResultSummary> UpsertNode<T>(T node, IAsyncTransaction tx, CancellationToken ct = default) where T : GraphNode
        {
            ct.ThrowIfCancellationRequested();
            var cypher = BuildUpsertNodeQuery(node);
            logger.LogInformation("upsert ({label}:{pk})", node.LabelName, node.GetPrimaryKeyValue());
            return await tx.RunWriteAsync(cypher.Query, cypher.Parameters);
        }


        public async Task<IEnumerable<IResultSummary>> UpsertNodes<T>(IEnumerable<T> nodes) where T : GraphNode
        {
            await using var session = neo4jDriver.AsyncSession();
            return await UpsertNodes(nodes, session, CancellationToken.None).ConfigureAwait(false);
        }

        public async Task<IEnumerable<IResultSummary>> UpsertNodes<T>(IEnumerable<T> nodes, CancellationToken ct) where T : GraphNode
        {
            await using var session = neo4jDriver.AsyncSession();
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
            catch
            {
                try
                {
                    await tx.RollbackAsync().ConfigureAwait(false);
                }
                catch
                {
                    /* ignore */
                }

                throw;
            }
        }

        public async Task<IEnumerable<IResultSummary>> UpsertNodes<T>(IEnumerable<T> nodes, IAsyncTransaction tx, CancellationToken ct = default) where T : GraphNode
        {
            var results = new List<IResultSummary>();
            foreach (var node in nodes)
            {
                // UpsertNode handles its own cancellation check; no need to throw each iteration here.
                var cursor = await UpsertNode(node, tx, ct).ConfigureAwait(false);
                results.Add(cursor);
            }

            return results;
        }

        /// <summary>
        /// Builds the Cypher MERGE/SET query used by all UpsertNode overloads.
        /// Centralizing the query removes duplication and keeps behavior consistent across session and transaction variants.
        /// </summary>
        private CypherQuery BuildUpsertNodeQuery<T>(T node) where T : GraphNode
        {
            // Build parameterized Cypher query and parameter map
            var parameters = new Dictionary<string, object?>
            {
                [node.GetPrimaryKeyName()] = node.GetPrimaryKeyValue(),
                ["displayName"] = node.DisplayName,
                ["now"] = DateTimeOffset.UtcNow,
                ["upserted"] = DateTimeOffset.UtcNow
            };

            var setClauses = new List<string>
            {
                $"n.{node.GetPrimaryKeyName()} = ${node.GetPrimaryKeyName()}",
                $"n.{node.NodeDisplayNameProperty} = $displayName"
            };

            // Recursive flattening logic for container properties
            void AddNodePropertiesRecursive(object? obj, int depth, string prefix = "")
            {
                if (obj == null || depth <= 0) return;
                var properties = obj.GetType().GetProperties()
                    .Where(p => p.GetCustomAttribute<NodePropertyAttribute>() != null);
                foreach (var property in properties)
                {
                    var attribute = property.GetCustomAttribute<NodePropertyAttribute>();
                    if (attribute == null || attribute.Exclude) continue;
                    var propertyName = attribute.PropertyName;
                    var value = property.GetValue(obj);
                    // Flatten container objects: [NodeProperty("")]
                    if (value != null
                        && string.IsNullOrWhiteSpace(propertyName)
                        && !IsSimpleType(value.GetType())
                        && !IsEnumerable(value.GetType()))
                    {
                        AddNodePropertiesRecursive(value, depth - 1, prefix);
                        continue;
                    }

                    // Always update upserted
                    if (!string.IsNullOrWhiteSpace(propertyName) && propertyName.Equals("upserted", StringComparison.InvariantCultureIgnoreCase))
                    {
                        parameters["upserted"] = DateTimeOffset.UtcNow;
                        setClauses.Add("n.upserted = $upserted");
                        continue;
                    }

                    // Special handling for IEnumerable<SequenceText>
                    if (!string.IsNullOrWhiteSpace(propertyName) && value is IEnumerable<SequenceText> seqEnum)
                    {
                        // Flatten SequenceText collection into a single comma separated string (requested behavior)
                        // Note: Neo4j does support list properties, but storing as a single string here per requirement.
                        var ordered = seqEnum
                            .Where(st => st != null && !string.IsNullOrWhiteSpace(st.Text))
                            .OrderBy(st => st.Sequence)
                            .Select(st => st.Text?.Trim())
                            .Where(t => !string.IsNullOrWhiteSpace(t))
                            .ToList();

                        var joined = ordered.Count == 0 ? null : string.Join(", ", ordered);
                        var paramKey = string.IsNullOrEmpty(prefix) ? propertyName : $"{prefix}_{propertyName}";
                        parameters[paramKey] = joined;
                        setClauses.Add($"n.{propertyName} = ${paramKey}");
                        continue;
                    }

                    // Special handling for single SequenceText instance
                    if (!string.IsNullOrWhiteSpace(propertyName) && value is SequenceText seqSingle)
                    {
                        var text = string.IsNullOrWhiteSpace(seqSingle.Text) ? null : seqSingle.Text.Trim();
                        var paramKey = string.IsNullOrEmpty(prefix) ? propertyName : $"{prefix}_{propertyName}";
                        parameters[paramKey] = text; // store primitive string or null
                        setClauses.Add($"n.{propertyName} = ${paramKey}");
                        continue;
                    }

                    // Parameterize all other values, skip if no propertyName
                    if (!string.IsNullOrWhiteSpace(propertyName))
                    {
                        var paramKey = string.IsNullOrEmpty(prefix) ? propertyName : $"{prefix}_{propertyName}";
                        parameters[paramKey] = value ?? null;
                        setClauses.Add($"n.{propertyName} = ${paramKey}");
                    }
                }
            }

            AddNodePropertiesRecursive(node, 5);

            var query = $$"""
                          MERGE (n:{{node.LabelName}} {{{node.GetPrimaryKeyName()}}: ${{node.GetPrimaryKeyName()}} })
                          ON CREATE SET n.created = $now
                          SET
                            {{string.Join(",\n  ", setClauses)}}
                          """;
            // Cast to required non-nullable dictionary type for CypherQuery
            var nonNullParams = new Dictionary<string, object>();
            foreach (var kv in parameters)
            {
                nonNullParams[kv.Key] = kv.Value!; // values can be null in Cypher params; driver accepts boxed nulls
            }
            return new CypherQuery(query, nonNullParams);
        }





        private static bool IsSimpleType(Type type)
        {
            return type.IsPrimitive
                   || type.IsEnum
                   || type == typeof(string)
                   || type == typeof(decimal);
        }

        static bool IsEnumerable(Type type)
        {
            return typeof(System.Collections.IEnumerable).IsAssignableFrom(type)
                   && type != typeof(string);
        }

        public async Task<bool> CreateRelationshipsAsync<T>(IEnumerable<T> fromNodes) where T : GraphNode
        {
            await using var session = neo4jDriver.AsyncSession();
            foreach (var node in fromNodes)
            {
                var result = await CreateRelationshipsAsync(node, session).ConfigureAwait(false);
                if (!result)
                    return false; // exit on failure. (may want to continue on failure of individual nodes?)
            }

            return true;
        }

        public async Task<bool> CreateRelationshipsAsync<T>(T fromNode) where T : GraphNode
        {
            await using var session = neo4jDriver.AsyncSession();
            return await CreateRelationshipsAsync(fromNode, session);
        }

        /// <summary>
        /// Creates relationships using an existing session. Caller is responsible for disposing the session.
        /// </summary>
        public async Task<bool> CreateRelationshipsAsync<T>(T fromNode, IAsyncSession session) where T : GraphNode
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
                    logger.LogError("RelationshipName is null or empty {NodeType}.", nodeType.Name);
                    return false;
                }
                if (string.IsNullOrWhiteSpace(relatedNodeTypeName))
                {
                    logger.LogError("relatedNodeType is null or empty {NodeType}.", nodeType.Name);
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
                    edgeSeeds = dataSourceService.GetSourceEdgesFor<CustomEdge>(seedEdgeType.Name).ToList();
                    if (!edgeSeeds.Any())
                    {
                        logger.LogWarning("EdgeSeed type {seedEdgeType} specified but no edge data found in DataSourceService.", seedEdgeType.Name);
                    }
                }

                // Simple relationships (string IDs)
                foreach (var toNodeKey in relatedNodeIds)
                {
                    // stock standard relationship with no properties on the edge
                    if (seedEdgeType == null)
                    {
                        await ExecuteCreateRelationshipsAsync(fromNode,
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
                        logger.LogWarning("Multiple ({count}) edge seeds found for {fromNode}-{toNode} on relationship {relationship}. Using first.",
                            nodeEdgeSeeds.Count, fromNode.GetPrimaryKeyValue(), toNodeKey, relationshipName);
                    }

                    var matchingEdge = nodeEdgeSeeds.First();

                    var edgeParameters = new Dictionary<string, object?>();
                    // create setClause and parameters for edge properties
                    var edgeType = matchingEdge.GetType();
                    var setClauses = new List<string>();
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

                    await ExecuteCreateRelationshipsAsync(fromNode,
                        session,
                        relatedNodeType,
                        relationshipName,
                        toNodeKey,
                        relatedNodeTypeName, edgeParameters, setClause);
                }
            }
            return true;
        }

        private async Task ExecuteCreateRelationshipsAsync<T>(T fromNode, IAsyncSession session, Type relatedNodeType, string relationshipName, 
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

            logger.LogInformation("(:{node} {from})-[{relationship}]->{to}", fromNode.LabelName, fromNode.GetPrimaryKeyValue(), relationshipName, toNodeKey);
            
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


        public async Task<bool> EnforceUniqueConstraints(IEnumerable<INodeService> nodeServices)
        {
            await using var session = neo4jDriver.AsyncSession();
            foreach (var nodeService in nodeServices)
            {
                if (!nodeService.EnforceUniqueConstraint)
                    continue;

                var type = nodeService.GetType();

                // Get the base type (e.g FileNodeService<Movie>)
                var baseType = type.BaseType;
                if (baseType == null || !baseType.IsGenericType)
                    continue;
                var genericType = baseType.GetGenericArguments()[0]; // e.g. typeof(Movie)
                if (Activator.CreateInstance(genericType) is GraphNode instance)
                {
                    var query = GetUniqueConstraintCypher(instance);
                    await ExecuteWriteQuery(session, query);
                }
            }

            return true;
        }

        /// <summary>
        /// Allows for multiple nodes having embeddings and vector indexes,
        /// however one is usually better when searching for semantic meaning across all data.
        /// </summary>
        /// <remarks>the nodes must have an "embedding" property.
        /// note: defaults to 3072 dimensions (for text-embedding-3-large).</remarks>
        public async Task<bool> CreateVectorIndexForEmbeddings(IList<string>? labelNames = null, int dimensions = 3072)
        {
            if (labelNames is null || !labelNames.Any())
                return true;

            /*
             * text-embedding-3-large | 3072 dimensions
             * text-embedding-ada-002 | 1536 dimensions
             * Important: If the embedding model changes, the index MUST be dropped and rebuilt!
             *
             * todo: auto set the dimensions based on the embedding model (used in AI layer)
             */

            var sw = System.Diagnostics.Stopwatch.StartNew();
            await using var session = neo4jDriver.AsyncSession();
            try
            {
                var cypherTemplate = await GetCypherFromFile("CreateVectorIndexForEmbeddings.cypher", logger);
                foreach (var labelName in labelNames)
                {
                    var cypher = cypherTemplate
                        .Replace("{labelName}", labelName.ToLower())
                        .Replace("{dimensions}", dimensions.ToString());
                    await ExecuteWriteQuery(session, cypher);
                }

                sw.Stop();
                logger.LogInformation("CreateVectorIndexForEmbeddings completed in {ElapsedMs}ms for {LabelCount} labels", sw.ElapsedMilliseconds, labelNames.Count);
                return true;
            }
            catch (Exception)
            {
                // catch and continue, will have been logged in ExecuteWriteQuery
                return false;
            }
        }

        public async Task<IEnumerable<string>> ExecuteReadListStringsAsync(string query, string returnObjectKey, IDictionary<string, object>? parameters = null)
        {
            await using var session = neo4jDriver.AsyncSession();
            try
            {
                parameters ??= new Dictionary<string, object>();

                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var records = await RunReadQueryValidateAlias(tx, query, parameters, returnObjectKey);
                    if (records.Count == 0)
                        return [];

                    // Assuming the returned value is a list of objects (like ["a", "b", "c"])
                    var list = records
                        .SelectMany(x => x[returnObjectKey].As<List<string>>())
                        .Distinct()
                        .ToList();
                    return list;
                });

                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Problem executing read list of strings. QueryLength={QueryLength} ParamKeys={ParamKeys}", query.Length,
                    string.Join(',', parameters?.Keys ?? Array.Empty<string>()));
                throw new RepositoryException("Read list (string) query failed.", query, parameters?.Keys ?? Array.Empty<string>(), ex);
            }
        }

        public async Task<IEnumerable<T>> ExecuteReadListAsync<T>(string query,
            string returnObjectKey, IDictionary<string, object>? parameters = null)
            where T : class, new()
        {
            // Maintains existing API while improving memory profile (no ToListAsync full materialization) and using compiled mapper
            await using var session = neo4jDriver.AsyncSession();
            return await ExecuteReadListAsync<T>(query, returnObjectKey, session, parameters);
        }

        public async Task<IEnumerable<T>> ExecuteReadListAsync<T>(string query,
            string returnObjectKey, IAsyncSession session, IDictionary<string, object>? parameters = null)
            where T : class, new()
        {
            try
            {
                parameters ??= new Dictionary<string, object>();

                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(query, parameters);
                    var list = new List<T>();
                    var aliasValidated = false;
                    var idProp = typeof(T).GetProperties()
                        .FirstOrDefault(p => string.Equals(p.Name, "Id", StringComparison.OrdinalIgnoreCase) && p.GetMethod != null);
                    var seen = idProp != null ? new HashSet<string>(StringComparer.OrdinalIgnoreCase) : null;

                    while (await cursor.FetchAsync())
                    {
                        var record = cursor.Current;
                        if (!aliasValidated)
                        {
                            if (!record.Keys.Contains(returnObjectKey))
                            {
                                var available = string.Join(", ", record.Keys);
                                throw new KeyNotFoundException(
                                    $"Return alias '{returnObjectKey}' not found. Available aliases: {available}. Ensure your Cypher uses 'RETURN <expr> AS {returnObjectKey}'. Query={query}");
                            }

                            aliasValidated = true;
                        }

                        var node = record[returnObjectKey].As<INode>();
                        var obj = MapNodeToObject<T>(node); // uses compiled mapper internally
                        if (seen != null)
                        {
                            var valObj = idProp!.GetValue(obj);
                            var key = valObj?.ToString() ?? string.Empty;
                            if (key.Length > 0 && !seen.Add(key))
                                continue; // skip duplicate
                        }

                        list.Add(obj);
                    }

                    return list;
                });

                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Problem executing read list. QueryLength={QueryLength} ParamKeys={ParamKeys}", query.Length, string.Join(',', parameters?.Keys ?? Array.Empty<string>()));
                throw new RepositoryException("Read list query failed.", query, parameters?.Keys ?? Array.Empty<string>(), ex);
            }
        }

        public async IAsyncEnumerable<T> ExecuteReadStreamAsync<T>(string query, string returnObjectKey, IDictionary<string, object>? parameters = null)
            where T : class, new()
        {
            // WARNING: If the consumer does not fully enumerate the stream, the session may not be disposed promptly.
            // Use 'await using' to ensure session disposal and prevent memory leaks.
            parameters ??= new Dictionary<string, object>();
            await using var session = neo4jDriver.AsyncSession();
            IResultCursor? cursor = null;
            try
            {
                cursor = await session.RunAsync(query, parameters);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Problem starting streamed read. QueryLength={QueryLength} ParamKeys={ParamKeys}", query.Length, string.Join(',', parameters.Keys));
                throw new RepositoryException("Read stream query failed (initialization).", query, parameters.Keys, ex);
            }

            var aliasValidated = false;
            while (await cursor.FetchAsync())
            {
                var record = cursor.Current;
                if (!aliasValidated)
                {
                    if (!record.Keys.Contains(returnObjectKey))
                    {
                        var available = string.Join(", ", record.Keys);
                        throw new KeyNotFoundException(
                            $"Return alias '{returnObjectKey}' not found. Available aliases: {available}. Ensure your Cypher uses 'RETURN <expr> AS {returnObjectKey}'. Query={query}");
                    }

                    aliasValidated = true;
                }

                var node = record[returnObjectKey].As<INode>();
                yield return MapNodeToObject<T>(node);
            }
        }

        /// <summary>
        /// Runs a read query within a transaction, materializes all records and validates the presence of the expected return alias.
        /// Returns an empty list if no records are found. Throws a KeyNotFoundException with detailed guidance when the alias is missing.
        /// </summary>
        /// <param name="tx">The active transaction.</param>
        /// <param name="query">Cypher query text.</param>
        /// <param name="parameters">Query parameters (must not be null).</param>
        /// <param name="returnObjectKey">Expected return alias.</param>
        /// <returns>List of records (possibly empty).</returns>
        /// <exception cref="KeyNotFoundException">Thrown when the alias is not present in the first record.</exception>
        private static async Task<List<IRecord>> RunReadQueryValidateAlias(IAsyncQueryRunner tx, string query, IDictionary<string, object> parameters, string returnObjectKey)
        {
            var cursor = await tx.RunAsync(query, parameters);
            var records = await cursor.ToListAsync();
            if (records.Count == 0)
                return [];

            // Validate alias presence to provide clearer error messages (moved here from individual methods for reuse)
            if (!records[0].Keys.Contains(returnObjectKey))
            {
                var available = string.Join(", ", records[0].Keys);
                throw new KeyNotFoundException(
                    $"Return alias '{returnObjectKey}' not found. Available aliases: {available}. Ensure your Cypher uses 'RETURN <expr> AS {returnObjectKey}'. Query={query}");
            }

            return records;
        }

        // Compiled mapper cache for hot paths (reduces per-record reflection cost)
        private static readonly ConcurrentDictionary<Type, Delegate> _compiledNodeMappers = new();
        private static readonly Regex _labelValidationRegex = new("^[A-Za-z0-9_]+$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

        private T MapNodeToObject<T>(INode node) where T : class
        {
            var mapper = (Func<INode, T>)_compiledNodeMappers.GetOrAdd(typeof(T), _ => BuildNodeMapper<T>());
            return mapper(node);
        }

        private Func<INode, T> BuildNodeMapper<T>() where T : class
        {
            var nodeParam = Expression.Parameter(typeof(INode), "node");
            var getPropsMethod = typeof(Neo4jGenericRepo).GetMethod(nameof(GetNodePropertiesDictionary), BindingFlags.Static | BindingFlags.NonPublic)!;
            var propsExpr = Expression.Call(getPropsMethod, nodeParam);
            var objVar = Expression.Variable(typeof(T), "obj");
            var valueVar = Expression.Variable(typeof(object), "val");
            var ctor = typeof(T).GetConstructor(Type.EmptyTypes);
            Expression createObjExpr = ctor != null
                ? Expression.New(ctor)
                : Expression.Convert(
                    Expression.Call(typeof(System.Runtime.CompilerServices.RuntimeHelpers).GetMethod("GetUninitializedObject", BindingFlags.Public | BindingFlags.Static)!, Expression.Constant(typeof(T))),
                    typeof(T));
            var assignObj = Expression.Assign(objVar, createObjExpr);
            var blockExpressions = new List<Expression> { assignObj };
            var tryGetValueMethod = typeof(IReadOnlyDictionary<string, object>).GetMethod("TryGetValue");

            foreach (var prop in typeof(T).GetProperties().Where(p => p.SetMethod != null && p.SetMethod.IsPublic))
            {
                try
                {
                    var attr = prop.GetCustomAttribute<NodePropertyAttribute>();
                    var candidates = new List<string>();
                    if (!string.IsNullOrWhiteSpace(attr?.PropertyName)) candidates.Add(attr!.PropertyName);
                    candidates.Add(prop.Name.ToGraphPropertyCasing());
                    if (!candidates.Contains(prop.Name)) candidates.Add(prop.Name); // raw fallback

                    Expression? candidateChain = null;
                    foreach (var candidate in candidates.Distinct(StringComparer.OrdinalIgnoreCase))
                    {
                        var tryGet = Expression.Call(propsExpr, tryGetValueMethod!, Expression.Constant(candidate), valueVar);
                        var notNull = Expression.NotEqual(valueVar, Expression.Constant(null));

                        Expression convertedValueExpr;
                        if (prop.PropertyType == typeof(float[]))
                        {
                            var helper = typeof(ValueConversionExtensions).GetMethod(nameof(ValueConversionExtensions.ConvertToFloatArray), BindingFlags.Static | BindingFlags.Public)!;
                            convertedValueExpr = Expression.Call(helper, valueVar);
                        }
                        else if (prop.PropertyType == typeof(Guid))
                        {
                            var helper = typeof(ValueConversionExtensions).GetMethod(nameof(ValueConversionExtensions.ConvertToGuid), BindingFlags.Static | BindingFlags.Public)!;
                            convertedValueExpr = Expression.Call(helper, valueVar);
                        }
                        else if (prop.PropertyType == typeof(List<string>))
                        {
                            var helper = typeof(ValueConversionExtensions).GetMethod(nameof(ValueConversionExtensions.ConvertToStringList), BindingFlags.Static | BindingFlags.Public)!;
                            convertedValueExpr = Expression.Call(helper, valueVar);
                        }
                        else if (prop.PropertyType == typeof(DateTimeOffset))
                        {
                            var helper = typeof(ValueConversionExtensions).GetMethod(nameof(ValueConversionExtensions.ConvertToDateTimeOffset), BindingFlags.Static | BindingFlags.Public)!;
                            convertedValueExpr = Expression.Call(helper, valueVar);
                        }
                        else if (prop.PropertyType == typeof(DateTime))
                        {
                            var helper = typeof(ValueConversionExtensions).GetMethod(nameof(ValueConversionExtensions.ConvertToDateTime), BindingFlags.Static | BindingFlags.Public)!;
                            convertedValueExpr = Expression.Call(helper, valueVar);
                        }
                        else
                        {
                            var changeType = typeof(Convert).GetMethod(nameof(Convert.ChangeType), [typeof(object), typeof(Type)])!;
                            convertedValueExpr = Expression.Convert(
                                Expression.Call(changeType, valueVar, Expression.Constant(prop.PropertyType)), prop.PropertyType);
                        }

                        var assignProp = Expression.Assign(Expression.Property(objVar, prop), convertedValueExpr);
                        var ifValNotNull = Expression.IfThen(notNull, assignProp);
                        var candidateIf = Expression.IfThen(tryGet, ifValNotNull);
                        candidateChain = candidateChain == null
                            ? candidateIf
                            : Expression.IfThenElse(tryGet, ifValNotNull, candidateChain);
                    }

                    if (candidateChain != null)
                        blockExpressions.Add(candidateChain);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error processing property {PropertyName}", prop.Name);
                }
            }

            blockExpressions.Add(objVar);
            var body = Expression.Block([objVar, valueVar], blockExpressions);
            return Expression.Lambda<Func<INode, T>>(body, nodeParam).Compile();
        }



        // Helper to obtain properties dictionary robustly across potential driver differences.
        private static IReadOnlyDictionary<string, object> GetNodePropertiesDictionary(INode node)
        {
            // Try interface/reflection first
            try
            {
                var propInfo = node.GetType().GetProperty("Properties");
                if (propInfo?.GetValue(node) is IReadOnlyDictionary<string, object> dictFromProp)
                    return dictFromProp;
            }
            catch
            {
                // ignore and fallback
            }

            // Fallback: build dictionary from Keys/Values if available
            try
            {
                var keysProp = node.GetType().GetProperty("Keys");
                var valuesProp = node.GetType().GetProperty("Values");
                if (keysProp?.GetValue(node) is IEnumerable<string> keys && valuesProp?.GetValue(node) is IEnumerable<object> values)
                {
                    var dict = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                    using var keyEnum = keys.GetEnumerator();
                    using var valEnum = values.GetEnumerator();
                    while (keyEnum.MoveNext() && valEnum.MoveNext())
                    {
                        if (keyEnum.Current != null)
                            dict[keyEnum.Current] = valEnum.Current;
                    }

                    return dict;
                }
            }
            catch
            {
                // ignore
            }

            return new Dictionary<string, object>();
        }

        /// <summary>
        /// Execute read scalar as an asynchronous operation.
        /// </summary>
        /// <remarks>untested - 20250424</remarks>
        public async Task<T> ExecuteReadScalarAsync<T>(string query, IDictionary<string, object>? parameters = null)
        {
            await using var session = neo4jDriver.AsyncSession();
            try
            {
                parameters = parameters ?? new Dictionary<string, object>();

                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(query, parameters);
                    var scalar = (await cursor.SingleAsync())[0].As<T>();
                    return scalar;
                });

                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Problem executing scalar read. QueryLength={QueryLength} ParamKeys={ParamKeys}", query.Length, string.Join(',', parameters?.Keys ?? Array.Empty<string>()));
                throw new RepositoryException("Read scalar query failed.", query, parameters?.Keys ?? Array.Empty<string>(), ex);
            }
        }

        #region Relationship Loading Helpers

        /// <summary>
        /// Retrieves metadata for List<string> properties decorated with NodeRelationshipAttribute on a given type.
        /// </summary>
        private static List<EdgeMeta> GetRelationshipMetadata(Type t)
        {
            var metas = new List<EdgeMeta>();
            var props = t.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            // Determine source label and pk (for FromId in pattern comprehensions)
            var sourceLabel = t.Name;
            var sourcePk = "id";
            try
            {
                if (Activator.CreateInstance(t) is GraphNode src)
                {
                    sourceLabel = src.LabelName;
                    sourcePk = src.GetPrimaryKeyName();
                }
            }
            catch { /* ignore, keep defaults */ }

            var idx = 0;
            foreach (var p in props)
            {
                if (!typeof(IEnumerable<string>).IsAssignableFrom(p.PropertyType))
                    continue;

                var attr = p.GetCustomAttributes()
                    .FirstOrDefault(a => a.GetType().IsGenericType && a.GetType().GetGenericTypeDefinition() == typeof(NodeRelationshipAttribute<>));
                if (attr == null) continue;

                var relName = attr.GetType().GetProperty("RelationshipName")?.GetValue(attr)?.ToString();
                if (string.IsNullOrWhiteSpace(relName))
                    continue;

                var targetType = attr.GetType().GetGenericArguments()[0];
                var targetPk = "id";
                var edgeObjectType = attr.GetType().GetProperty("SeedEdgeType")?.GetValue(attr) as Type;

                try
                {
                    if (Activator.CreateInstance(targetType) is GraphNode gn)
                        targetPk = gn.GetPrimaryKeyName();
                }
                catch { /* ignore */ }

                var alias = $"edge{idx}";
                var objAlias = $"edge{idx}_obj";

                metas.Add(new EdgeMeta(
                    Property: p,
                    edgeName: relName!,
                    SourceLabel: sourceLabel,
                    SourcePrimaryKey: sourcePk,
                    TargetLabel: targetType.Name,
                    TargetPrimaryKey: targetPk,
                    Alias: alias,
                    ObjAlias: objAlias,
                    EdgeObjectType: edgeObjectType
                ));

                idx++;
            }

            return metas;
        }


        /// <summary>
        /// Builds a single Cypher query to load node(s) and aggregate id lists, with optional edge-object maps via pattern comprehensions.
        /// </summary>
        /// <param name="label">Node label</param>
        /// <param name="sourcePkName">Source node PK name</param>
        /// <param name="relationships">Relationship metadata</param>
        /// <param name="filterObject">Inline "{ pk: $id }" map or null</param>
        /// <param name="skip">Optional SKIP</param>
        /// <param name="limit">Optional LIMIT</param>
        /// <param name="includeEdgeObjects">If true, returns map arrays for edges that have EdgeObjectType</param>
        /// <param name="includeEdges">Optional filter; match by property name or relationship name (case-insensitive)</param>
        private static string BuildLoadQuery(
            string label,
            string sourcePkName,
            List<EdgeMeta> relationships,
            string? filterObject,
            int? skip,
            int? limit,
            bool includeEdgeObjects = false,
            ISet<string>? includeEdges = null)
        {
            bool IsIncluded(EdgeMeta r)
            {
                if (!includeEdgeObjects) return false;
                if (r.EdgeObjectType is null) return false;
                if (includeEdges == null || includeEdges.Count == 0) return true;
                // Allow filtering by id-list property name OR relationship type OR target label
                return includeEdges.Contains(r.Property.Name, StringComparer.OrdinalIgnoreCase)
                       || includeEdges.Contains(r.edgeName, StringComparer.OrdinalIgnoreCase)
                       || includeEdges.Contains(r.TargetLabel, StringComparer.OrdinalIgnoreCase);
            }

            var matchFilter = string.IsNullOrWhiteSpace(filterObject) ? string.Empty : " " + filterObject.Trim();
            var sb = new System.Text.StringBuilder();

            if (relationships.Count == 0)
            {
                sb.Append($"MATCH (n:{label}{matchFilter}) RETURN n");
            }
            else
            {
                sb.Append($"MATCH (n:{label}{matchFilter})\n");

                // Existing id list aggregation via OPTIONAL MATCH + collect
                for (var i = 0; i < relationships.Count; i++)
                {
                    var r = relationships[i];
                    sb.Append($"OPTIONAL MATCH (n)-[:{r.edgeName}]->(relNode{i}:{r.TargetLabel})\n");
                    sb.Append($"WITH n, collect(DISTINCT relNode{i}.{r.TargetPrimaryKey}) AS {r.Alias}");
                    if (i > 0)
                    {
                        var carry = string.Join(", ", relationships.Take(i).Select(m => m.Alias));
                        sb.Append(", ").Append(carry);
                    }
                    sb.Append('\n');
                }

                sb.Append("RETURN n");
                foreach (var r in relationships)
                    sb.Append($", {r.Alias}");

                // Pattern comprehension for edge-object maps (opt-in)
                for (var i = 0; i < relationships.Count; i++)
                {
                    var r = relationships[i];
                    if (!IsIncluded(r)) continue;

                    // rel var names unique per relationship
                    var relVar = $"rel{i}";
                    var toVar = $"to{i}";
                    // Provide both generic FromId/ToId and strongly named keys like MovieId/GenreId for convenience
                    var srcIdKey = $"{r.SourceLabel}Id";
                    var tgtIdKey = $"{r.TargetLabel}Id";

                    sb.Append($", [ (n)-[{relVar}:{r.edgeName}]->({toVar}:{r.TargetLabel}) | ");
                    sb.Append($"{relVar} {{ .*");
                    sb.Append($", FromId: n.{sourcePkName}, ToId: {toVar}.{r.TargetPrimaryKey}");
                    sb.Append($", {srcIdKey}: n.{sourcePkName}, {tgtIdKey}: {toVar}.{r.TargetPrimaryKey}");
                    sb.Append(" } ] AS ").Append(r.ObjAlias);
                }
            }

            if (skip.HasValue)
                sb.Append(" SKIP $skip");
            if (limit.HasValue)
                sb.Append(" LIMIT $take");

            return sb.ToString();
        }

        /// <summary>
        /// Back-compat wrapper (no edge objects).
        /// </summary>
        private static string BuildLoadQuery(string label, string sourcePkName, List<EdgeMeta> edges, string? filterObject)
            => BuildLoadQuery(label, sourcePkName, edges, filterObject, null, null, false, null);

        /// <summary>
        /// Back-compat wrapper (no edge objects) with paging.
        /// </summary>
        private static string BuildLoadQuery(string label, string sourcePkName, List<EdgeMeta> relationships, string? filterObject, int? skip, int? limit)
            => BuildLoadQuery(label, sourcePkName, relationships, filterObject, skip, limit, false, null);

        /// <summary>
        /// Maps aggregated relationship id collections to List<string> properties.
        /// </summary>
        private void MapRelationshipLists<T>(IRecord record, T entity, List<EdgeMeta> relationships) where T : GraphNode
        {
            foreach (var r in relationships)
            {
                if (!record.Keys.Contains(r.Alias)) continue;
                try
                {
                    var list = record[r.Alias].As<List<object>>().Select(o => o?.ToString() ?? string.Empty)
                        .Where(s => !string.IsNullOrEmpty(s))
                        .Distinct()
                        .ToList();
                    r.Property.SetValue(entity, list);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed mapping relationship list for {Type}.{Prop}", typeof(T).Name, r.Property.Name);
                }
            }
        }

        /// <summary>
        /// Hydrates CustomEdge subclasses from map arrays returned by pattern comprehensions.
        /// </summary>
        private void MapEdgeObjects<T>(IRecord record, T entity, List<EdgeMeta> relationships) where T : GraphNode
        {
            var entityType = entity!.GetType();

            foreach (var r in relationships.Where(r => r.EdgeObjectType != null))
            {
                if (!record.Keys.Contains(r.ObjAlias)) continue;

                List<object>? maps;
                try
                {
                    maps = record[r.ObjAlias].As<List<object>>();
                }
                catch
                {
                    maps = null;
                }
                if (maps == null || maps.Count == 0) continue;

                // Find destination property of type List<TEdge>
                var edgeType = r.EdgeObjectType!;
                var destProp = entityType
                    .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .FirstOrDefault(p => IsListOf(p.PropertyType, edgeType));
                if (destProp == null) continue; // no target property to assign

                var listType = typeof(List<>).MakeGenericType(edgeType);
                var list = (System.Collections.IList)Activator.CreateInstance(listType)!;

                foreach (var m in maps)
                {
                    if (m is not IDictionary<string, object> dict) continue;
                    var edgeObj = Activator.CreateInstance(edgeType);
                    if (edgeObj == null) continue;

                    foreach (var prop in edgeType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                    {
                        if (!prop.CanWrite) continue;
                        if (Attribute.IsDefined(prop, typeof(EdgePropertyIgnoreAttribute))) continue;

                        // Try exact property name match first
                        if (TryGetCaseInsensitive(dict, prop.Name, out var val))
                        {
                            TryAssign(edgeObj, prop, val);
                            continue;
                        }

                        // Fallbacks for id naming
                        if (prop.Name.Equals($"{r.SourceLabel}Id", StringComparison.OrdinalIgnoreCase)
                            && TryGetCaseInsensitive(dict, $"{r.SourceLabel}Id", out var fromStrong))
                        {
                            TryAssign(edgeObj, prop, fromStrong);
                            continue;
                        }

                        if (prop.Name.Equals($"{r.TargetLabel}Id", StringComparison.OrdinalIgnoreCase)
                            && TryGetCaseInsensitive(dict, $"{r.TargetLabel}Id", out var toStrong))
                        {
                            TryAssign(edgeObj, prop, toStrong);
                            continue;
                        }

                        if (prop.Name.Equals("FromId", StringComparison.OrdinalIgnoreCase)
                            && TryGetCaseInsensitive(dict, "FromId", out var fromGeneric))
                        {
                            TryAssign(edgeObj, prop, fromGeneric);
                            continue;
                        }

                        if (prop.Name.Equals("ToId", StringComparison.OrdinalIgnoreCase)
                            && TryGetCaseInsensitive(dict, "ToId", out var toGeneric))
                        {
                            TryAssign(edgeObj, prop, toGeneric);
                            continue;
                        }
                    }

                    list.Add(edgeObj);
                }

                destProp.SetValue(entity, list);
            }

            static bool TryGetCaseInsensitive(IDictionary<string, object> dict, string key, out object? value)
            {
                foreach (var k in dict.Keys)
                {
                    if (k.Equals(key, StringComparison.OrdinalIgnoreCase))
                    {
                        value = dict[k];
                        return true;
                    }
                }
                value = null;
                return false;
            }

            static bool IsListOf(Type candidate, Type elementType)
            {
                if (!candidate.IsGenericType) return false;
                var gen = candidate.GetGenericTypeDefinition();
                if (gen == typeof(List<>) || gen == typeof(IList<>) || gen == typeof(IEnumerable<>))
                {
                    var arg = candidate.GetGenericArguments()[0];
                    return arg.IsAssignableFrom(elementType) || elementType.IsAssignableFrom(arg);
                }
                return false;
            }

            static void TryAssign(object target, PropertyInfo prop, object? value)
            {
                try
                {
                    if (value == null)
                    {
                        prop.SetValue(target, null);
                        return;
                    }

                    var destType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

                    // Handle common numeric conversions (Neo4j integers come back as long)
                    if (destType == typeof(int) && value is long l) { prop.SetValue(target, (int)l); return; }
                    if (destType == typeof(long) && value is long ll) { prop.SetValue(target, ll); return; }
                    if (destType == typeof(double) && value is double d) { prop.SetValue(target, d); return; }
                    if (destType == typeof(float) && value is double fd) { prop.SetValue(target, (float)fd); return; }
                    if (destType == typeof(bool) && value is bool b) { prop.SetValue(target, b); return; }
                    if (destType == typeof(string)) { prop.SetValue(target, value.ToString()); return; }

                    var converted = Convert.ChangeType(value, destType);
                    prop.SetValue(target, converted);
                }
                catch
                {
                    // best-effort; ignore assignment failures
                }
            }
        }

        #endregion

        public async Task<T?> LoadAsync<T>(string id, CancellationToken ct = default) where T : GraphNode, new()
        {
            ct.ThrowIfCancellationRequested();
            if (string.IsNullOrWhiteSpace(id)) throw new ArgumentException("id required", nameof(id));

            var temp = new T();
            var label = temp.LabelName;
            var pkName = temp.GetPrimaryKeyName();

            var relationships = GetRelationshipMetadata(typeof(T));
            var query = BuildLoadQuery(label, pkName, relationships, $"{{ {pkName}: $id }}", null, null, false, null);

            await using var session = neo4jDriver.AsyncSession();
            try
            {
                var records = await session.ExecuteReadAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(query, new { id });
                    return await cursor.ToListAsync(cancellationToken: ct);
                });

                if (records.Count == 0) return null;
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
                logger.LogError(ex, "Failed loading node {Label}:{Id}", label, id);
                throw new RepositoryException($"Failed loading node {label}:{id}", query, ["id"], ex);
            }
        }

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

            await using var session = neo4jDriver.AsyncSession();
            try
            {
                var records = await session.ExecuteReadAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(query, parameters);
                    return await cursor.ToListAsync(cancellationToken: ct);
                });

                if (records.Count == 0) return Array.Empty<T>();
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
                logger.LogError(ex, "Failed loading all nodes for {Label}", label);
                throw new RepositoryException($"Failed loading all nodes for {label}", query, Array.Empty<string>(), ex);
            }
        }

        // New opt-in overloads to include edge-object loading
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

            await using var session = neo4jDriver.AsyncSession();
            try
            {
                var records = await session.ExecuteReadAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(query, new { id });
                    return await cursor.ToListAsync(cancellationToken: ct);
                });

                if (records.Count == 0) return null;
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
                logger.LogError(ex, "Failed loading node {Label}:{Id}", label, id);
                throw;
            }
        }

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

            await using var session = neo4jDriver.AsyncSession();
            try
            {
                var records = await session.ExecuteReadAsync(async tx =>
                {
                    var cursor = await tx.RunAsync(query, parameters);
                    return await cursor.ToListAsync(cancellationToken: ct);
                });

                if (records.Count == 0) return Array.Empty<T>();
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
                logger.LogError(ex, "Failed loading all nodes for {Label}", label);
                throw;
            }
        }
        /// <summary>
        /// Generic helper to traverse from a source node to related nodes through a set of relationship types
        /// using a variable length path and return the distinct related nodes mapped to the requested type.
        /// </summary>
        /// <remarks>
        /// Cypher pattern generated:
        /// MATCH (s:SourceLabel { pk: $id })-[:REL1|REL2*min..max]->(t:TargetLabel)
        /// RETURN DISTINCT t AS node
        /// Validation enforces simple safe relationship tokens (alphanumeric & underscore). Relationship names are
        /// upper‑cased to align with existing conventions (see ToGraphRelationShipCasing). Designed for read paths only.
        /// </remarks>
        public async Task<IReadOnlyList<TRelated>> LoadRelatedNodesAsync<TSource, TRelated>(string sourceId, string relationshipTypes, int minHops = 1, int maxHops = 4, IAsyncTransaction? tx = null,
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
        /// Reusable internal helper to execute a read query expected to return a single node per record
        /// (under a specified alias) and map results to <typeparamref name="T"/> using the compiled mapper.
        /// Distinct filtering by Id (if present) is applied client-side. Intended for lightweight list lookups.
        /// </summary>
        /// <typeparam name="T">Graph node type to materialize.</typeparam>
        /// <param name="query">Cypher query text.</param>
        /// <param name="parameters">Query parameters (nullable -> empty).</param>
        /// <param name="returnAlias">Alias of the node in the RETURN clause (default 'node').</param>
        /// <param name="runner">Optional existing transaction/session runner; if null a temp session is created.</param>
        /// <param name="ct">Cancellation token.</param>
        private async Task<IReadOnlyList<T>> ExecuteReadNodeQueryAsync<T>(string query, IDictionary<string, object>? parameters, string returnAlias, IAsyncQueryRunner? runner, CancellationToken ct)
            where T : GraphNode
        {
            parameters ??= new Dictionary<string, object>();

            async Task<IReadOnlyList<T>> InnerAsync(IAsyncQueryRunner r)
            {
                try
                {
                    var cursor = await r.RunAsync(query, parameters);
                    var list = new List<T>();
                    while (await cursor.FetchAsync())
                    {
                        var record = cursor.Current;
                        if (!record.Keys.Contains(returnAlias)) continue;
                        var node = record[returnAlias].As<INode>();
                        var mapped = MapNodeToObject<T>(node);
                        list.Add(mapped);
                    }

                    var idProp = typeof(T).GetProperty("Id", BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                    if (idProp != null)
                    {
                        list = list
                            .GroupBy(o => idProp.GetValue(o)?.ToString(), StringComparer.OrdinalIgnoreCase)
                            .Select(g => g.First())
                            .ToList();
                    }

                    return list;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogError(ex, "ExecuteReadNodeQueryAsync failure. Alias={Alias} QueryLength={QueryLength}", returnAlias, query.Length);
                    throw new RepositoryException("Failed executing node list read.", query, parameters.Keys, ex);
                }
            }

            if (runner != null)
            {
                return await InnerAsync(runner);
            }

            await using var session = neo4jDriver.AsyncSession();
            return await session.ExecuteReadAsync(async tx => await InnerAsync(tx));
        }

        /// <summary>
        /// Lightweight variant that returns only the distinct id values of related nodes rather than hydrating full node objects.
        /// Supports traversing outgoing, incoming or undirected (both) relationships.
        /// </summary>
        public async Task<IReadOnlyList<string>> LoadRelatedNodeIdsAsync<TRelated>(GraphNode fromNode, string relationshipTypes, int minHops = 1, int maxHops = 4,
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
                    logger.LogError(ex, "LoadRelatedNodeIdsAsync failure. QueryLength={QueryLength}", query.Length);
                    throw new RepositoryException("Failed executing related node id list read.", query, parameters.Keys, ex);
                }
            }

            if (tx != null)
            {
                return await ExecAsync(tx);
            }

            await using var session = neo4jDriver.AsyncSession();
            return await session.ExecuteReadAsync(async rtx => await ExecAsync(rtx));
        }

        /// <summary>
        /// Enforces a unique constraint on a node
        /// </summary>
        private string GetUniqueConstraintCypher<T>(T node) where T : GraphNode
        {
            logger.LogInformation("CREATE UNIQUE CONSTRAINT {node}", node.LabelName);
            return $"""
                    CREATE CONSTRAINT {node.LabelName.ToLower()}_{node.GetPrimaryKeyName().ToLower()}_is_unique IF NOT EXISTS
                    FOR (n:{node.LabelName})
                    REQUIRE n.{node.GetPrimaryKeyName()} IS UNIQUE
                    """;
        }


        public async Task<NodeRelationshipsResponse> GetAllNodesAndRelationshipsAsync()
        {
            await using var session = neo4jDriver.AsyncSession();
            return await GetAllNodesAndRelationshipsAsync(session);
        }

        /// <summary>
        /// Get a list of the names of all node types and their relationships (in and out).
        /// WARNING: For large graphs, materializing all nodes/relationships may cause high memory usage.
        /// Consider streaming or paginating results if this method is used on large datasets.
        /// </summary>
        /// <remarks>Ensure the session is appropriately disposed of! Caller Responsibility.</remarks>
        /// <returns>Useful if you want to feed your graph map into AI</returns>
        public async Task<NodeRelationshipsResponse> GetAllNodesAndRelationshipsAsync(IAsyncSession session)
        {
            if (session == null) throw new ArgumentNullException(nameof(session));
            // why exclude? I pass the result into AI to help it gen cypher. If a rel exists on many NodeType's, to minimize noise (and cost) I pass this instead: 
            // example: "GlobalOutgoingRelationships": ["IN_GROUP"]
            var excludedOutRelationships = config.GetSection("Neo4jLiteRepo:GetNodesAndRelationships:excludedOutRelationships")
                .Get<List<string>>() ?? [];
            var excludedInRelationships = config.GetSection("Neo4jLiteRepo:GetNodesAndRelationships:excludedInRelationships")
                .Get<List<string>>() ?? [];

            // Create a parameters dictionary
            var parameters = new Dictionary<string, object>
            {
                { "excludedOutRels", excludedOutRelationships },
                { "excludedInRels", excludedInRelationships }
            };

            var query = await GetCypherFromFile("GetAllNodesAndRelationships.cypher", logger);

            IResultCursor cursor;
            try
            {
                cursor = await session.RunAsync(query, parameters);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Problem running GetNodesAndRelationships query. QueryLength={QueryLength} ParamKeys={ParamKeys}", query.Length, string.Join(',', parameters.Keys));
                throw new RepositoryException("Get nodes and relationships failed.", query, parameters.Keys, ex);
            }

            try
            {
                var records = await cursor.ToListAsync();
                var response = new NodeRelationshipsResponse
                {
                    QueriedAt = DateTime.UtcNow,
                    NodeTypes = records.Select(record => new NodeRelationshipInfo
                    {
                        NodeType = record["NodeType"].As<string>(),
                        OutgoingRelationships = record["OutgoingRelationships"].As<List<string>>(),
                        IncomingRelationships = record["IncomingRelationships"].As<List<string>>()
                    }).ToList()
                };
                return response;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Problem materializing records for GetNodesAndRelationships. QueryLength={QueryLength}", query.Length);
                throw new RepositoryException("Get nodes and relationships materialization failed.", query, parameters.Keys, ex);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await neo4jDriver.DisposeAsync();
        }

        /// <summary>
        /// Executes a vector similarity search query to find relevant content chunks
        /// </summary>
        /// <param name="questionEmbedding">The embedding vector of the question</param>
        /// <param name="topK">Number of most relevant chunks to return</param>
        /// <param name="includeContext">Whether to include related chunks and parent context</param>
        /// <param name="similarityThreshold">Minimum cosine similarity threshold (0-1) for matching content. 
        /// Higher values (e.g. 0.8) will return only very close matches and may result in fewer results.
        /// Lower values (e.g. 0.5) will return more results but may include less relevant content.
        /// Values between 0.6-0.75 are typically a good starting point.</param>
        /// <returns>A list of strings containing the content and article information</returns>
        public async Task<List<string>> ExecuteVectorSimilaritySearchAsync(
            float[] questionEmbedding,
            int topK = 25,
            bool includeContext = true,
            double similarityThreshold = 0.65)
        {
            var query = await GetCypherFromFile("ExecuteVectorSimilaritySearch.cypher", logger);

            await using var session = neo4jDriver.AsyncSession();
            List<string> result;
            try
            {
                result = await session.ExecuteReadAsync(async tx =>
                {
                    // Run the query
                    var cursor = await tx.RunAsync(query, new
                    {
                        questionEmbedding,
                        topK
                    });

                    // Process results
                    var resultsDict = new Dictionary<string, Dictionary<string, object>>();

                    await foreach (var record in cursor)
                    {
                        var id = record["id"].As<string>();

                        // If we've already seen this chunk, skip it (avoid duplicates)
                        if (resultsDict.ContainsKey(id))
                            continue;

                        resultsDict[id] = new Dictionary<string, object>
                        {
                            ["id"] = id,
                            ["content"] = record["content"].As<string>(),
                            ["articleTitle"] = record["articleTitle"].As<string>(),
                            ["articleUrl"] = record["articleUrl"].As<string>(),
                            ["score"] = record["score"].As<double>(),
                            ["entities"] = record["entities"].As<List<string>>(),
                            ["sequenceOrder"] = record["sequenceOrder"].As<int>(),
                            ["resultType"] = record["resultType"].As<string>()
                        };
                    }

                    // Sort by article title and sequence order for better readability
                    var sortedResults = resultsDict.Values
                        .OrderBy(r => r["articleTitle"].ToString())
                        .ThenBy(r => (int)r["sequenceOrder"])
                        .ToList();

                    // Format results to return
                    var formattedResults = new List<string>();

                    var currentArticle = "";
                    foreach (var r in sortedResults)
                    {
                        var articleTitle = r["articleTitle"]?.ToString() ?? "Unknown Article";
                        var articleUrl = r["articleUrl"]?.ToString() ?? "no-link";

                        // If we're starting a new article, add a header
                        if (articleTitle != currentArticle)
                        {
                            if (formattedResults.Count > 0)
                                formattedResults.Add($"-- end article: {currentArticle} --"); // Add a clear delimiter between articles for LLM context

                            formattedResults.Add($"-- start article: {articleTitle} --");
                            formattedResults.Add($"article url: {articleUrl}");
                            currentArticle = articleTitle;
                        }

                        // Add content with prefix based on result type
                        var prefix = "";
                        var resultType = r["resultType"]?.ToString() ?? "unknown";

                        if (resultType == "main")
                            prefix = "▶️ "; // Highlight the main matches
                        else if (resultType == "next")
                            prefix = "⏩ "; // Context that follows
                        else if (resultType == "previous")
                            prefix = "⏪ "; // Context that precedes
                        else if (resultType == "sibling")
                            prefix = "🔄 "; // Related content
                        else if (resultType == "section_related")
                            prefix = "📑 "; // Section-related content
                        else if (resultType == "subsection_related")
                            prefix = "📋 "; // SubSection-related content

                        var entities = r["entities"] as List<string> ?? new List<string>();
                        var entityInfo = entities.Any() ? $" [Entities: {string.Join(", ", entities)}]" : "";

                        var content = $"{prefix}{r["content"]}{entityInfo}";
                        if (!formattedResults.Contains(content) && !string.IsNullOrWhiteSpace(content))
                            formattedResults.Add(content);
                    }

                    return formattedResults;
                });
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Vector similarity search failed. QueryLength={QueryLength} ParamKeys=questionEmbedding,topK", query.Length);
                throw new RepositoryException("Vector similarity search failed.", query, new[] { "questionEmbedding", "topK" }, ex);
            }

            return result;
        }

        /// <inheritdoc />
        public async Task<int> RemoveOrphansAsync<T>(CancellationToken ct = default) where T : GraphNode, new()
        {
            await using var session = neo4jDriver.AsyncSession();
            // Delegate to the session overload to keep all write orchestration logic centralized.
            return await RemoveOrphansAsync<T>(session, ct);
        }


        /// <inheritdoc />
        public async Task<int> RemoveOrphansAsync<T>(IAsyncSession session, CancellationToken ct = default) where T : GraphNode, new()
        {
            if (session == null) throw new ArgumentNullException(nameof(session));
            await using var tx = await session.BeginTransactionAsync();
            try
            {
                var result = await RemoveOrphansAsync<T>(tx, ct);
                await tx.CommitAsync();
                return result;
            }
            catch
            {
                try
                {
                    await tx.RollbackAsync();
                }
                catch
                {
                    /* ignore */
                }

                throw;
            }
        }

        /// <inheritdoc />
        public async Task<int> RemoveOrphansAsync<T>(IAsyncTransaction tx, CancellationToken ct = default) where T : GraphNode, new()
        {
            if (tx == null) throw new ArgumentNullException(nameof(tx));
            var temp = new T();
            var label = temp.LabelName;
            ValidateLabel(label, nameof(label));

            // Batch delete orphans to avoid memory spikes
            const int batchSize = 400; // configurable if needed
            var totalDeleted = 0;
            var cypher = $$"""
                           MATCH (n:{{label}}) WHERE NOT (n)--() WITH n LIMIT $batchSize DETACH DELETE n RETURN count(n) AS deleted
                           """;


            try
            {
                while (true)
                {
                    var cursor = await tx.RunAsync(cypher, new { batchSize });
                    if (await cursor.FetchAsync())
                    {
                        var deleted = cursor.Current["deleted"].As<int>();
                        totalDeleted += deleted;
                        if (deleted < batchSize)
                        {
                            // Last batch, done
                            break;
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                return totalDeleted;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "RemoveOrphansAsync failure. QueryLength={QueryLength}", cypher.Length);
                throw new RepositoryException("Failed removing orphans.", cypher, ["label"], ex);
            }
        }

        /// <summary>
        /// Gets a Cypher query from a .cypher file in the Queries directory
        /// </summary>
        /// <param name="fileName">The name of the .cypher file without path</param>
        /// <param name="logger">Logger instance for logging messages</param>
        /// <returns>The contents of the Cypher query file as a string</returns>
        /// <remarks>why put the cypher in a file? One reason, proper formatting in editors.</remarks>
        /// <exception cref="FileNotFoundException">Thrown when the query file cannot be found</exception>
        private static async Task<string> GetCypherFromFile(string fileName, ILogger<Neo4jGenericRepo> logger)
        {
            // Read the Cypher query from file
            var queryFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Queries", fileName);

            // If file doesn't exist at runtime location, try to find it relative to the source code
            if (!File.Exists(queryFilePath))
            {
                var projectPath = Path.GetDirectoryName(typeof(Neo4jGenericRepo).Assembly.Location);
                while (projectPath != null && !Directory.Exists(Path.Combine(projectPath, "Queries")))
                {
                    projectPath = Directory.GetParent(projectPath)?.FullName;
                }

                if (projectPath != null)
                {
                    queryFilePath = Path.Combine(projectPath, "Queries", fileName);
                }
            }

            // Load query from file
            if (File.Exists(queryFilePath))
            {
                var query = await File.ReadAllTextAsync(queryFilePath);
                logger.LogDebug("Loaded Cypher query from file: {FilePath} {length}", queryFilePath, query.Length);
                if (query.Length == 0)
                    throw new RepositoryException("Cypher query file is empty.", queryFilePath, ["fileName"], new FileNotFoundException("Cypher query file is empty.", queryFilePath));
                return query;
            }
            else
            {
                // Log error and throw exception
                logger.LogError("Could not find Cypher query file at {FilePath}", queryFilePath);
                throw new RepositoryException("Cypher query file not found.", queryFilePath, ["fileName"], new FileNotFoundException("Cypher query file not found.", queryFilePath));
            }
        }

    }
}