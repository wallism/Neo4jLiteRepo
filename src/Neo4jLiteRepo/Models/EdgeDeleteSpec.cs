namespace Neo4jLiteRepo.Models;

/// <summary>
/// Specification for a relationship to delete (single directed or undirected edge between two nodes).
/// </summary>
/// <param name="FromNode">Source node.</param>
/// <param name="Rel">Relationship type.</param>
/// <param name="ToNode">Target node.</param>
/// <param name="Direction">Direction of the relationship to match (Outgoing / Incoming / Both).</param>
public record EdgeDeleteSpec(GraphNode FromNode, string Rel, GraphNode ToNode, EdgeDirection Direction);