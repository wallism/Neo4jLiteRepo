namespace Neo4jLiteRepo.Attributes;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class ExcludeFromTypeGenerationAttribute : Attribute
{
}