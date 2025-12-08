namespace Neo4jLiteRepo.Models;

/// <summary>
/// Represents relationship metadata for a specific node type in the graph.
/// Used to describe outgoing and incoming relationship types for each node label.
/// </summary>
public class NodeRelationshipInfo
{
    public override string ToString() => $"{NodeType} Out:{OutgoingRelationships.Count} In:{IncomingRelationships.Count}";

    /// <summary>
    /// The label or type name of the node (e.g., "Movie", "Person").
    /// </summary>
    public string? NodeType { get; set; }

    /// <summary>
    /// List of relationship type names that originate from this node type (outgoing edges).
    /// </summary>
    public List<string> OutgoingRelationships { get; set; } = [];

    /// <summary>
    /// List of relationship type names that target this node type (incoming edges).
    /// </summary>
    public List<string> IncomingRelationships { get; set; } = [];
}