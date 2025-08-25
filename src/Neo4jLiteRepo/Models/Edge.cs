namespace Neo4jLiteRepo.Models;

/// <summary>
/// Represents a relationship between two nodes in the graph.
/// Inherit from this when your edge needs custom properties.
/// </summary>
public interface IEdge
{
    string TargetPrimaryKey { get; set; }
}

/// <inheritdoc />
public abstract class Edge : IEdge
{
    public override string ToString() => $"to:{TargetPrimaryKey}";
    public required string TargetPrimaryKey { get; set; }
}