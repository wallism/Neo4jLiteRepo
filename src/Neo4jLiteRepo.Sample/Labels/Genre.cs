using Neo4jLiteRepo.Attributes;

namespace Neo4jLiteRepo.Sample.Labels
{
    public class Genre : SampleGraphNode
    {

        /// <summary>
        /// Name of the Genre
        /// </summary>
        /// <remarks>Id would be a better PK, using Name for the sample for
        /// human readability (easier to see relationships in the json)</remarks>
        [NodePrimaryKey]
        public required string Name { get; set; }

        [NodeProperty(nameof(Description))]
        public string Description { get; set; }
        

        [NodeRelationship<Movie>("HAS_MOVIE")] 
        public IEnumerable<string> Movies { get; set; }

        public override string BuildDisplayName()=> Name;
    }
}