using Neo4jLiteRepo.Helpers;

namespace Neo4jLiteRepo.Attributes;

[AttributeUsage(AttributeTargets.Property)]
// ReSharper disable once UnusedTypeParameter : not used within the attribute, but it is used by the GenericRepo
public class NodeRelationshipAttribute<T>(string relationshipName) : Attribute 
    where T : GraphNode
{

    // ReSharper disable once UnusedMember.Global : used by the GenericRepo
    public string RelationshipName { get; } = relationshipName.ToGraphRelationShipCasing();
}