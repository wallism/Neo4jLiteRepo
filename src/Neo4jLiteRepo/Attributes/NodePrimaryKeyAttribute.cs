namespace Neo4jLiteRepo.Attributes;

[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class NodePrimaryKeyAttribute : Attribute;