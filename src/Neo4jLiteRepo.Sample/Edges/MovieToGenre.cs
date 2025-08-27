using Neo4jLiteRepo.Models;
using Neo4jLiteRepo.Sample.Nodes;

namespace Neo4jLiteRepo.Sample.Edges;

/// <summary>
/// The edge from Movie to Genre has custom properties.
/// </summary>
public class MovieGenre
{
    public Genre Genre { get; set; }
    public string SampleEdgeProperty { get; set; }
}

/// <summary>
/// Only used for Seeding the database.
/// </summary>
[Obsolete("not actually obsolete, but do not use for anything except seeding.")]
public class MovieGenreEdgeSeed : EdgeSeed
{
    public override string FromId => MovieId;
    public override string ToId => GenreId;

    [EdgePropertyIgnore]
    public string MovieId { get; set; }
    [EdgePropertyIgnore]
    public string GenreId { get; set; }

    public string SampleEdgeProperty { get; set; }
}

