using Neo4jLiteRepo.Attributes;

namespace Neo4jLiteRepo.Sample.Nodes
{
    public class Movie : SampleGraphNode
    {
        public override required string Id { get; set; }

        [NodePrimaryKey]
        public string Title { get; set; }

        [NodeProperty(nameof(Released))]
        public int Released { get; set; }

        [NodeProperty(nameof(Tagline))]
        public string Tagline { get; set; }

        [NodeProperty(nameof(TestPassword))]
        public string TestPassword { get; set; }

        [NodeRelationship<Genre>("IN_GENRE")]
        public IEnumerable<string> Genres { get; set; }


        [NodeProperty("TestArray")]
        public List<string> TestArray { get; set; }


        public override string BuildDisplayName() => Title;
    }
} 