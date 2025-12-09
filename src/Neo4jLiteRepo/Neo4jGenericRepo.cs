using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Attributes;
using Neo4jLiteRepo.Exceptions;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.Models;
using Neo4jLiteRepo.NodeServices;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
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
        /// Executes the structured vector similarity search returning strongly typed rows (no string formatting side-effects).
        /// </summary>
        Task<IReadOnlyList<StructuredVectorSearchRow>> ExecuteVectorSimilaritySearchStructuredAsync(
            float[] questionEmbedding,
            int topK = 20,
            double similarityThreshold = 0.65);
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
        /// Get a list of the names of all labels (node types) and their edges (in and out) as JSON.
        /// </summary>
        /// <remarks>Useful if you want to feed your graph map into AI.</remarks>
        Task<string> GetGraphMapAsJsonAsync();

        /// <summary>
        /// Retrieves all nodes and their relationships
        /// </summary>
        Task<NodeRelationshipsResponse> GetAllNodesAndEdgesAsync();

        /// <summary>
        /// Retrieves all nodes and their relationships using the provided session.
        /// </summary>
        Task<NodeRelationshipsResponse> GetAllNodesAndEdgesAsync(IAsyncSession session);


        /// <summary>
        /// Executes a read query and returns a list of objects of type T.
        /// </summary>
        Task<IEnumerable<T>> ExecuteReadListAsync<T>(string query, string returnObjectKey, 
            IDictionary<string, object>? parameters = null)
            where T : class, new();

        Task<IEnumerable<T>> ExecuteReadListAsync<T>(string query, string returnObjectKey, 
            IAsyncSession session, IDictionary<string, object>? parameters = null)
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

    /// <summary>
    /// Generic repository for Neo4j graph database operations.
    /// This partial class contains the core infrastructure: constructor, fields, Dispose, and private helpers.
    /// Method implementations are split across partial class files for maintainability.
    /// </summary>
    public partial class Neo4jGenericRepo : IAsyncDisposable, INeo4jGenericRepo
    {
        private readonly ILogger<Neo4jGenericRepo> _logger;
        private readonly IConfiguration _config;
        private readonly IDriver _neo4jDriver;
        private readonly IDataSourceService _dataSourceService;
        private readonly string? _databaseName;

        public Neo4jGenericRepo(
            ILogger<Neo4jGenericRepo> logger,
            IConfiguration config,
            IDriver neo4jDriver,
            IDataSourceService dataSourceService)
        {
            _logger = logger;
            _config = config;
            _neo4jDriver = neo4jDriver;
            _dataSourceService = dataSourceService;
            _databaseName = config["Neo4jSettings:Database"];
        }

        #region Dispose

        /// <inheritdoc/>
        public void Dispose()
        {
            _neo4jDriver.Dispose();
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            await _neo4jDriver.DisposeAsync();
        }

        #endregion

        #region Session Management

        protected string Now => DateTimeOffset.Now.ToLocalTime().ToString("O");

        /// <inheritdoc/>
        public IAsyncSession StartSession()
        {
            return string.IsNullOrEmpty(_databaseName) 
                ? _neo4jDriver.AsyncSession() 
                : _neo4jDriver.AsyncSession(o => o.WithDatabase(_databaseName));
        }

        #endregion

        #region Write Query Helpers

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
                _logger.LogError(authEx, "ExecuteWriteQuery auth error (runner).");
                throw;
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("[OperationCanceled] ExecuteWriteQuery auth error (runner).");
                throw;
            }
            catch (Exception ex)
            {
                // only write to console in case there are secrets in the query
                Console.WriteLine($"**** write query failed ****{query}");
                _logger.LogError(ex, "ExecuteWriteQuery (runner) failure. QueryLength={QueryLength}", query.Length);
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
                _logger.LogError(authEx, "ExecuteWriteQuery auth error (runner/param).");
                throw;
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("[OperationCanceled] ExecuteWriteQuery auth error (runner/param).");
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"**** write query failed ****{query}");
                _logger.LogError(ex, "ExecuteWriteQuery (runner/param) failure. QueryLength={QueryLength}", query.Length);
                // Attempt to surface parameter property names (best-effort)
                var paramNames = parameters.GetType().GetProperties().Select(p => p.Name).ToArray();
                throw new RepositoryException("Failed executing write query (runner/param).", query, paramNames, ex);
            }
        }

        #endregion

        #region Validation Helpers

        private static readonly Regex _labelValidationRegex = new("^[A-Za-z0-9_]+$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

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

        private bool IsInDetachDeleteWhitelist(string label)
        {
            // Only allow certain labels to be deleted via this method to avoid accidental mass deletes.
            _logger.LogWarning("ZERO labels defined on whitelist"); // todo: make configurable
            return false;
        }

        #endregion

        #region Upsert Query Builder

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

            List<string> setClauses =
            [
                $"n.{node.GetPrimaryKeyName()} = ${node.GetPrimaryKeyName()}",
                $"n.{node.NodeDisplayNameProperty} = $displayName"
            ];

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

        #endregion

        #region Read Query Helpers

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

        #endregion

        #region Node Mapping

        // Compiled mapper cache for hot paths (reduces per-record reflection cost)
        private static readonly ConcurrentDictionary<Type, Delegate> _compiledNodeMappers = new();

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
            List<Expression> blockExpressions = [assignObj];
            var tryGetValueMethod = typeof(IReadOnlyDictionary<string, object>).GetMethod("TryGetValue");

            foreach (var prop in typeof(T).GetProperties().Where(p => p.SetMethod != null && p.SetMethod.IsPublic))
            {
                try
                {
                    var attr = prop.GetCustomAttribute<NodePropertyAttribute>();
                    List<string> candidates = [];
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
                        else if (prop.PropertyType.FullName == "Neo4jLiteRepo.Models.SequenceText")
                        {
                            var helper = typeof(ValueConversionExtensions).GetMethod(nameof(ValueConversionExtensions.ConvertToSequenceText), BindingFlags.Static | BindingFlags.Public)!;
                            convertedValueExpr = Expression.Call(helper, valueVar);
                        }
                        else if (prop.PropertyType.IsGenericType && prop.PropertyType.GetGenericTypeDefinition() == typeof(List<>) &&
                                 prop.PropertyType.GetGenericArguments()[0].FullName == "Neo4jLiteRepo.Models.SequenceText")
                        {
                            var helper = typeof(ValueConversionExtensions).GetMethod(nameof(ValueConversionExtensions.ConvertToSequenceTextList), BindingFlags.Static | BindingFlags.Public)!;
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
                    _logger.LogError(ex, "Error processing property {PropertyName}", prop.Name);
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
            catch (Exception)
            {
                // ignore and fallback - static method cannot access logger
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
            catch (Exception)
            {
                // ignore - static method cannot access logger
            }

            return new Dictionary<string, object>();
        }

        #endregion

        #region Relationship Loading Helpers

        /// <summary>
        /// Retrieves metadata for List<string> properties decorated with NodeRelationshipAttribute on a given type.
        /// </summary>
        private static List<EdgeMeta> GetRelationshipMetadata(Type t)
        {
            List<EdgeMeta> metas = [];
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
                    _logger.LogWarning(ex, "Failed mapping relationship list for {Type}.{Prop}", typeof(T).Name, r.Property.Name);
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
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Failed to get edge objects for alias {Alias}", r.ObjAlias);
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
                catch (Exception)
                {
                    // best-effort; ignore assignment failures - static local function cannot access logger
                }
            }
        }

        #endregion

        #region Cypher File Loading

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

        #endregion
    }
}

/// <summary>
/// Row returned from structured vector similarity search.
/// </summary>
public sealed class StructuredVectorSearchRow
{
    public string ChunkId { get; set; } = string.Empty;
    public string ArticleTitle { get; set; } = string.Empty;
    public string ArticleUrl { get; set; } = string.Empty;
    public string SnippetType { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;
    public double BaseScore { get; set; }
    public int Sequence { get; set; }
    public List<string> Entities { get; set; } = new();
}
