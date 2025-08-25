using Neo4jLiteRepo;
using Neo4jLiteRepo.Attributes;
using Neo4jLiteRepo.Models;
using System.Collections.Generic;

namespace Neo4jLiteRepo.Sample.Nodes
{
    public class Person : GraphNode
    {
        [NodePrimaryKey]
        public string Name { get; set; }

        [NodeProperty("BirthYear")]
        public int BirthYear { get; set; }

        [NodeRelationship<Movie>("ACTED_IN")]
        public IEnumerable<string> ActedIn { get; set; }

        public override string BuildDisplayName() => Name;
        public override string GetMainContent() => $"{Name} ({BirthYear})";
    }
}
