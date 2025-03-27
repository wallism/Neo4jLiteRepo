﻿using Neo4jLiteRepo.Attributes;
using Neo4jLiteRepo.Helpers;
using System.Reflection;

namespace Neo4jLiteRepo;

public abstract class GraphNode
{
    public override string ToString() => $"{DisplayName}";

    /// <summary>
    /// By default, the LabelName is the name of the implementation class
    /// </summary>
    public virtual string LabelName 
        => GetType().Name.ToPascalCase();


    /// <summary>
    /// Name of the "Display Name" property on the Node.
    /// </summary>
    /// <remarks>not necessary but it is useful to have a nice brief display field which tells you what the node is.</remarks>
    public virtual string NodeDisplayNameProperty => nameof(DisplayName).ToGraphPropertyCasing();
    
    public string DisplayName => BuildDisplayName();

    /// <summary>
    /// Unique identifier for the node
    /// </summary>
    public virtual required string Id { get; set; }
    
    
    /// <summary>
    /// Override to manipulate the name before it is used as the display name
    /// </summary>
    public abstract string BuildDisplayName();

    private PropertyInfo? _primaryKeyProperty;

    /// <summary>
    /// Name of the "Primary Key" property on the Node.
    /// </summary>
    /// <remarks>neo4j doesn't have a concept of a primary key.
    /// This is essentially the main field that will be used when finding matches.</remarks>
    public string GetPrimaryKeyName()
    {
        var pkProperty = GetPrimaryKeyProperty();
         
        return pkProperty.Name.ToGraphPropertyCasing();
    }

    public virtual string? GetPrimaryKeyValue()
    {
        var primaryKeyProperty = GetPrimaryKeyProperty();

        var primaryKeyValue = primaryKeyProperty.GetValue(this);
        if (primaryKeyValue == null)
            throw new InvalidOperationException($"The primary key property '{primaryKeyProperty.Name}' on {GetType().Name} is null.");

        return primaryKeyValue.ToString();
    }

    private PropertyInfo GetPrimaryKeyProperty()
    {
        var pkProperties = GetType().GetProperties()
            .Where(p => p.GetCustomAttribute<NodePrimaryKeyAttribute>() != null);

        if (pkProperties == null)
            throw new InvalidOperationException($"No property decorated with [NodePrimaryKey] found on {GetType().Name}.");

        // If we allow abstract classes, the attribute will likely be declared on concrete classes too,
        // then the logic that uses this attribute may not work as expected, because it returns the FIRST property it finds with this attribute.
        // If the base class is the ONLY declaration, then allow it.
        if (pkProperties.Count() > 1 && pkProperties.Any(p => p.DeclaringType?.IsAbstract ?? false))
            throw new InvalidOperationException("NodePrimaryKeyAttribute cannot be applied to abstract classes.");

        _primaryKeyProperty ??= pkProperties.First(p => p.GetCustomAttribute<NodePrimaryKeyAttribute>() != null);


        return _primaryKeyProperty;
    }
}