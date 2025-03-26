using Neo4jLiteRepo.Helpers;

namespace Neo4jLiteRepo;

public abstract class GraphNode
{
    /// <summary>
    /// By default, the LabelName is the name of the implementation class
    /// </summary>
    public virtual string LabelName 
        => GetType().Name.ToPascalCase();

    /// <summary>
    /// Name of the "Primary Key" property on the Node.
    /// </summary>
    /// <remarks>neo4j doesn't have a concept of a primary key.
    /// This is essentially the main field that will be used when finding matches.</remarks>
    public virtual string NodePrimaryKeyName => nameof(Name).ToGraphPropertyCasing();

    /// <summary>
    /// Name of the "Display Name" property on the Node.
    /// </summary>
    /// <remarks>not necessary but it is useful to have a nice brief display field which tells you what the node is.</remarks>
    public virtual string NodeDisplayName => nameof(DisplayName).ToGraphPropertyCasing();

    /// <summary>
    /// Unique identifier for the node (name may not be unique)
    /// </summary>
    public abstract string Id { get; set; }

    /// <summary>
    /// Name value
    /// </summary>
    /// <remarks>if this is null, in the implementing class, make sure it has "override"</remarks>
    public abstract string Name { get; set; }
    
    
    public virtual string GetNodePrimaryKeyValue() => Name;

    public virtual string DisplayName => BuildDisplayName();
    
    /// <summary>
    /// Override to manipulate the name before it is used as the display name
    /// </summary>
    public virtual string BuildDisplayName() => Name;

}