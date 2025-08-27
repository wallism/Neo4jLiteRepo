using Neo4jLiteRepo.Attributes;

namespace Neo4jLiteRepo.Sample.Nodes
{
    public class Person : GraphNode
    {
        /// <summary>
        /// Gets or sets the unique identifier for the Person.
        /// </summary>
        [NodePrimaryKey]
        public string Id { get; set; }

        [NodeProperty("Name")]
        public string Name { get; set; }

        [NodeProperty("BirthYear")]
        public int BirthYear { get; set; }

        [NodeRelationship<Movie>("ACTED_IN")]
        public IEnumerable<string> ActedIn { get; set; }

        public override string BuildDisplayName() => Name;
        public override string GetMainContent() => $"{Name} ({BirthYear})";
    }
}
