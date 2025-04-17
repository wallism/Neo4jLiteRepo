namespace Neo4jLiteRepo.Models;

public class NodeRelationshipInfo
{
    public string NodeType { get; set; }
    public List<string> OutgoingRelationships { get; set; } = [];
    public List<string> IncomingRelationships { get; set; } = [];
}