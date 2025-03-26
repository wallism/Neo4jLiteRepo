using Neo4jLiteRepo.Attributes;
using Neo4jLiteRepo.Helpers;

namespace Neo4jLiteRepo.Sample.Labels
{
    public class Movie : SampleGraphNode
    {
        public override string Id { get; set; }
        public override string Name { get; set; }
        public string Title { get; set; }

        [NodeProperty(nameof(Released))]
        public int Released { get; set; }

        [NodeProperty(nameof(Tagline))]
        public string Tagline { get; set; }

        [NodeRelationship<Genre>("IN_GENRE")]
        public IEnumerable<string> Genres { get; set; }


        public override string NodePrimaryKeyName => nameof(Title).ToGraphPropertyCasing();
        public override string BuildDisplayName() => Title;
    }
} 