using Neo4jLiteRepo.Helpers;

namespace Neo4jLiteRepo.Attributes;

/// <summary>
/// Attribute for marking properties on node models as relationships to other node types in Neo4j.
/// Used by Neo4jGenericRepo to discover and manage graph relationships between nodes.
/// The generic type parameter T specifies the related node type.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
// ReSharper disable once UnusedTypeParameter : not used within the attribute, but it is used by the GenericRepo
public class NodeRelationshipAttribute<T>(string relationshipName, Type? edgeSeedType = null) : Attribute 
    where T : GraphNode
{

    // ReSharper disable once UnusedMember.Global : used by the GenericRepo
    public string RelationshipName { get; } = relationshipName.ToGraphRelationShipCasing();

    public Type? SeedEdgeType { get; set; } = edgeSeedType;
}