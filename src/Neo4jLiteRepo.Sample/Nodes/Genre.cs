using Neo4jLiteRepo.Attributes;

namespace Neo4jLiteRepo.Sample.Nodes
{
    public class Genre : SampleGraphNode
    {

        /// <summary>
        /// Gets or sets the unique identifier for the Genre.
        /// </summary>
        [NodePrimaryKey]
        public required string Id { get; set; }

        /// <summary>
        /// Name of the Genre
        /// </summary>
        public required string Name { get; set; }

        [NodeProperty(nameof(Description))]
        public string Description { get; set; }
        
        [NodeProperty(nameof(MySecretTest))]
        public string MySecretTest { get; set; }
        
        [NodeRelationship<Movie>("HAS_MOVIE")] 
        public IEnumerable<string> Movies { get; set; }

        public override string BuildDisplayName()=> Name;

        public override string GetMainContent() => $"{Description}";
    }
}