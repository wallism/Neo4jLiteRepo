namespace Neo4jLiteRepo.Models;

public class NodeRelationshipsResponse
{
    public List<NodeRelationshipInfo> NodeTypes { get; set; } = [];
    public DateTime QueriedAt { get; set; }
}