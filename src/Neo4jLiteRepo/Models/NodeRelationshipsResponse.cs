namespace Neo4jLiteRepo.Models;

/// <summary>
/// Response model containing all node types and their relationship metadata for a graph query.
/// Used by Neo4jGenericRepo to return node/relationship structure snapshots.
/// </summary>
public class NodeRelationshipsResponse
{
    /// <summary>
    /// List of node types and their relationship info.
    /// </summary>
    public List<NodeRelationshipInfo> NodeTypes { get; set; } = [];

    /// <summary>
    /// Timestamp when the relationship data was queried.
    /// </summary>
    public DateTime QueriedAt { get; set; }
}