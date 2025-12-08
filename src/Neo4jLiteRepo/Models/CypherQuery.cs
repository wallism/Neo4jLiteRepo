namespace Neo4jLiteRepo.Models;

/// <summary>
/// Represents a Cypher query and its parameters for Neo4j operations.
/// </summary>
public record CypherQuery(string Query, IDictionary<string, object> Parameters);