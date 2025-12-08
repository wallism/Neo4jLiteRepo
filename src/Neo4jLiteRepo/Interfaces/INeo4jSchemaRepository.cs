using Neo4j.Driver;
using Neo4jLiteRepo.Models;
using Neo4jLiteRepo.NodeServices;

namespace Neo4jLiteRepo.Interfaces;

/// <summary>
/// Schema and index management operations for Neo4j.
/// </summary>
public interface INeo4jSchemaRepository
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
    /// Get a list of the names of all labels (node types) and their edges (in and out) as JSON.
    /// </summary>
    /// <remarks>Useful if you want to feed your graph map into AI.</remarks>
    Task<string> GetGraphMapAsJsonAsync();

    /// <summary>
    /// Retrieves all nodes and their relationships.
    /// </summary>
    Task<NodeRelationshipsResponse> GetAllNodesAndEdgesAsync();

    /// <summary>
    /// Retrieves all nodes and their relationships using the provided session.
    /// </summary>
    Task<NodeRelationshipsResponse> GetAllNodesAndEdgesAsync(IAsyncSession session);
}
