using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Exceptions;
using Neo4jLiteRepo.Models;
using Neo4jLiteRepo.NodeServices;
using System.Text.Json;

namespace Neo4jLiteRepo;

/// <summary>
/// Schema and index operations for Neo4j.
/// </summary>
public partial class Neo4jGenericRepo
{
    #region EnforceUniqueConstraints

    /// <inheritdoc/>
    public async Task<bool> EnforceUniqueConstraints(IEnumerable<INodeService> nodeServices)
    {
        await using var session = StartSession();
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
    /// Enforces a unique constraint on a node
    /// </summary>
    private string GetUniqueConstraintCypher<T>(T node) where T : GraphNode
    {
        _logger.LogInformation("CREATE UNIQUE CONSTRAINT {node}", node.LabelName);
        return $"""
                CREATE CONSTRAINT {node.LabelName.ToLower()}_{node.GetPrimaryKeyName().ToLower()}_is_unique IF NOT EXISTS
                FOR (n:{node.LabelName})
                REQUIRE n.{node.GetPrimaryKeyName()} IS UNIQUE
                """;
    }

    #endregion

    #region CreateVectorIndexForEmbeddings

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
        await using var session = StartSession();
        try
        {
            var cypherTemplate = await GetCypherFromFile("CreateVectorIndexForEmbeddings.cypher", _logger);
            foreach (var labelName in labelNames)
            {
                var cypher = cypherTemplate
                    .Replace("{labelName}", labelName.ToLower())
                    .Replace("{dimensions}", dimensions.ToString());
                await ExecuteWriteQuery(session, cypher);
            }

            sw.Stop();
            _logger.LogInformation("CreateVectorIndexForEmbeddings completed in {ElapsedMs}ms for {LabelCount} labels", sw.ElapsedMilliseconds, labelNames.Count);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create vector index for embeddings");
            return false;
        }
    }

    #endregion

    #region GetGraphMapAsJsonAsync

    /// <inheritdoc />
    public async Task<string> GetGraphMapAsJsonAsync()
    {
        try
        {
            // Get current node types and relationships from the database
            var graphStructure = await GetAllNodesAndEdgesAsync();

            // Create a structured object with the graph information
            var finalStructure = new
            {
                GlobalProperties = (string[])["displayName", "upserted"],
                NodeTypes = graphStructure.NodeTypes.Select(n => new
                {
                    Name = n.NodeType,
                    OutgoingRelationships = n.OutgoingRelationships,
                    IncomingRelationships = n.IncomingRelationships
                }).ToList()
            };
            // Serialize to JSON with minimal indentation
            var result = JsonSerializer.Serialize(finalStructure, new JsonSerializerOptions
            {
                WriteIndented = false
            });

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GetGraphDbNodesAndEdgesJsonAsync");

            // default to a minimal structure
            var structure = $$""" 
                              {
                                  "GlobalProperties": [ "displayName", "upserted" ],
                                  "NodeTypes": [ ]
                              }
                              """;
            // Serialize to JSON with minimal indentation
            var result = JsonSerializer.Serialize(structure, new JsonSerializerOptions
            {
                WriteIndented = false
            });

            return result;
        }
    }

    #endregion

    #region GetAllNodesAndEdgesAsync

    /// <summary>
    /// Get a list of the names of all labels (node types) and their edges (in and out).
    /// Not any node data, just the types and relationships.
    /// </summary>
    /// <remarks>Ensure the session is appropriately disposed of! Caller Responsibility.</remarks>
    /// <returns>Useful if you want to feed your graph map into AI</returns>
    public async Task<NodeRelationshipsResponse> GetAllNodesAndEdgesAsync()
    {
        await using var session = StartSession();
        return await GetAllNodesAndEdgesAsync(session);
    }

    /// <summary>
    /// Get a list of the names of all labels (node types) and their edges (in and out).
    /// Not any node data, just the types and relationships.
    /// </summary>
    /// <remarks>Ensure the session is appropriately disposed of! Caller Responsibility.</remarks>
    /// <returns>Useful if you want to feed your graph map into AI</returns>
    public async Task<NodeRelationshipsResponse> GetAllNodesAndEdgesAsync(IAsyncSession session)
    {
        if (session == null) throw new ArgumentNullException(nameof(session));
        // why exclude? I pass the result into AI to help it gen cypher. If a rel exists on many NodeType's, to minimize noise (and cost) I pass this instead: 
        // example: "GlobalOutgoingRelationships": ["IN_GROUP"]
        var excludedOutRelationships = _config.GetSection("Neo4jLiteRepo:GetNodesAndRelationships:excludedOutRelationships")
            .Get<List<string>>() ?? [];
        var excludedInRelationships = _config.GetSection("Neo4jLiteRepo:GetNodesAndRelationships:excludedInRelationships")
            .Get<List<string>>() ?? [];

        // Create a parameters dictionary
        var parameters = new Dictionary<string, object>
        {
            { "excludedOutRels", excludedOutRelationships },
            { "excludedInRels", excludedInRelationships }
        };

        var query = await GetCypherFromFile("GetAllNodesAndRelationships.cypher", _logger);

        IResultCursor cursor;
        try
        {
            cursor = await session.RunAsync(query, parameters);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Problem running GetNodesAndRelationships query. QueryLength={QueryLength} ParamKeys={ParamKeys}", query.Length, string.Join(',', parameters.Keys));
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
            _logger.LogError(ex, "Problem materializing records for GetNodesAndRelationships. QueryLength={QueryLength}", query.Length);
            throw new RepositoryException("Get nodes and relationships materialization failed.", query, parameters.Keys, ex);
        }
    }

    #endregion
}
