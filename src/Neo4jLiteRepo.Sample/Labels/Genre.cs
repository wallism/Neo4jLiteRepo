using Neo4jLiteRepo.Attributes;

namespace Neo4jLiteRepo.Sample.Labels
{
    public class Genre : SampleGraphNode
    {
        public override required string Id { get; set; }
        public override required string Name { get; set; }

        [NodeProperty(nameof(Description))]
        public string Description { get; set; }
        

        [NodeRelationship<Movie>("HAS_MOVIE")] 
        public IEnumerable<string> Movies { get; set; }

    }
}