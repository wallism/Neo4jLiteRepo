using System.Reflection;

namespace Neo4jLiteRepo.Models;

internal sealed record RelationshipMeta(PropertyInfo Property, string RelationshipName, string TargetLabel, string TargetPrimaryKey, string Alias);

