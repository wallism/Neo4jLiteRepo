using System.Reflection;

namespace Neo4jLiteRepo.Models;

internal sealed record EdgeMeta(
    PropertyInfo Property,
    string edgeName,
    string SourceLabel,
    string SourcePrimaryKey,
    string TargetLabel,
    string TargetPrimaryKey,
    string Alias,
    string ObjAlias,
    Type? EdgeObjectType);

