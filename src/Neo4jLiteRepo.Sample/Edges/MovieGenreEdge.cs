using Neo4jLiteRepo.Models;
using Neo4jLiteRepo.Sample.Nodes;

namespace Neo4jLiteRepo.Sample.Edges;


/// <summary>
/// Only used for Seeding the database.
/// </summary>
public class MovieGenreEdge : CustomEdge
{
    public override string GetFromId() => MovieId;
    public override string GetToId() => GenreId;

    public string MovieId { get; set; }
 
    public string GenreId { get; set; }

    public string SampleEdgeProperty { get; set; }
}

