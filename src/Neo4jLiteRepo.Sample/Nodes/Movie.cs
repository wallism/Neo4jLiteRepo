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
        public required string Id { get; set; }

        [NodeProperty("Title")]
        public string Title { get; set; }

        [NodeProperty(nameof(Released))]
        public int Released { get; set; }

        [NodeProperty(nameof(Tagline))]
        public string Tagline { get; set; }

        [NodeProperty(nameof(TestPassword))]
        public string TestPassword { get; set; }

        [NodeRelationship<Genre>("IN_GENRE")]
        public IEnumerable<MovieToGenre> Genres { get; set; }


        [NodeProperty("TestArray")]
        public List<string> TestArray { get; set; }


        public override string BuildDisplayName() => Title;

        public override string GetMainContent() => $"{Title}";
    }
} 