using Neo4jLiteRepo.Models;

namespace Neo4jLiteRepo.Sample.Edges;

public class MovieToGenre : Edge
{
    public override string ToString() => $"to:{TargetPrimaryKey} sample:{SampleEdgeProperty}";

    public string SampleEdgeProperty { get; set; }
}