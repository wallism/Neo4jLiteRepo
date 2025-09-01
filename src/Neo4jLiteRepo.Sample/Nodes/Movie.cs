using Neo4jLiteRepo.Attributes;
using Neo4jLiteRepo.Sample.Edges;

namespace Neo4jLiteRepo.Sample.Nodes
{
    public class Movie : SampleGraphNode
    {

        /// <summary>
        /// Gets or sets the unique identifier for the Movie.
        /// </summary>
        [NodePrimaryKey]
        public string Id { get; set; }

        [NodeProperty("Title")]
        public string Title { get; set; }

        [NodeProperty(nameof(Released))]
        public int Released { get; set; }

        [NodeProperty(nameof(Tagline))]
        public string Tagline { get; set; }

        [NodeProperty(nameof(TestPassword))]
        public string TestPassword { get; set; }

        [NodeProperty("TestArray")]
        public List<string> TestArray { get; set; }

        [NodeRelationship<Genre>(Edges.InGenre, typeof(MovieGenreEdge))]
        public IEnumerable<string> GenreIds { get; set; } = [];
        
        public List<MovieGenreEdge>? InGenreEdges { get; set; }
        

        public override string BuildDisplayName() => Title;

        public override string GetMainContent() => $"{Title}";


        public static class Edges
        {
            public const string InGenre = "IN_GENRE";
        }
    }
} 