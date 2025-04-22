using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Attributes;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.Models;
using Neo4jLiteRepo.NodeServices;

namespace Neo4jLiteRepo
{
    public interface INeo4jGenericRepo : IDisposable
    {
        Task<bool> EnforceUniqueConstraints(IEnumerable<INodeService> nodeServices);

        Task<bool> UpsertNodes<T>(IEnumerable<T> nodes) where T : GraphNode;
        Task<bool> UpsertNode<T>(T node) where T : GraphNode;
        Task<bool> UpsertNode<T>(T node, IAsyncSession session) where T : GraphNode;

        Task<bool> CreateRelationshipsAsync<T>(IEnumerable<T> fromNodes) where T : GraphNode;
        Task<bool> CreateRelationshipsAsync<T>(T nodes) where T : GraphNode;
        Task<bool> CreateRelationshipsAsync<T>(T nodes, IAsyncSession session) where T : GraphNode;

        Task<NodeRelationshipsResponse> GetNodesAndRelationshipsAsync();
        Task<NodeRelationshipsResponse> GetNodesAndRelationshipsAsync(IAsyncSession session);

    }

    public class Neo4jGenericRepo(ILogger<Neo4jGenericRepo> logger,
        IConfiguration config,
        IDriver neo4jDriver,
        IDataSourceService dataSourceService) : IAsyncDisposable, INeo4jGenericRepo
    {
        public void Dispose()
        {
            neo4jDriver.Dispose();
        }
        
        protected string Now => DateTimeOffset.Now.ToLocalTime().ToString("O");

        public async Task<bool> UpsertNodes<T>(IEnumerable<T> nodes) where T : GraphNode
        {
            await using var session = neo4jDriver.AsyncSession();
            foreach (var node in nodes)
            {
                var result = await UpsertNode(node, session).ConfigureAwait(false);
                if (!result)
                    return false; // exit on failure. (may want to continue on failure of individual nodes?)
            }
            return true;
        }

        public async Task<bool> UpsertNode<T>(T node) where T : GraphNode
        {
            await using var session = neo4jDriver.AsyncSession();
            return await UpsertNode(node, session);
        }

        public async Task<bool> UpsertNode<T>(T node, IAsyncSession session) where T : GraphNode
        {
            logger.LogInformation("({label}:{node})", node.LabelName, node.DisplayName);
            var declaredNodeProperties = GetNodeProperties(node);

            var query = $$"""
                          MERGE (n:{{node.LabelName}} {{{node.GetPrimaryKeyName()}}: "{{node.GetPrimaryKeyValue()}}"})
                          ON CREATE SET
                            n.created = $Now
                          SET
                            n.id = "{{node.Id}}",
                            n.{{node.NodeDisplayNameProperty}} = "{{node.DisplayName}}",
                            {{declaredNodeProperties}},
                          
                            n.upserted = $Now

                          """;
            try
            {
                var result = await ExecuteWriteQuery(session, query);
                return result;
            }
            catch (AuthenticationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"UpsertNode {node.DisplayName} {query}", query);
                // don't throw, try to process other nodes
                return false;
            }
        }


        private static string GetNodeProperties<T>(T node, int depth = 5) where T : GraphNode
        {
            var declaredNodeProperties = GetNodePropertiesRecursive(node, depth);
            return string.Join(",\n  ", declaredNodeProperties);
        }

        private static IEnumerable<string> GetNodePropertiesRecursive(object? obj, int depth)
        {
            if (obj == null || depth <= 0)
                yield break;

            var properties = obj.GetType().GetProperties()
                .Where(p => p.GetCustomAttribute<NodePropertyAttribute>() != null);

            foreach (var property in properties)
            {
                var value = property.GetValue(obj);
                if (value != null && !IsSimpleType(value.GetType()))
                {
                    // Recurse into child objects
                    foreach (var childProperty in GetNodePropertiesRecursive(value, depth - 1))
                    {
                        yield return childProperty;
                    }
                }

                var attribute = property.GetCustomAttribute<NodePropertyAttribute>();
                if (attribute == null)
                    continue;
                if(attribute.Exclude)
                    continue;
                var propertyName = attribute.PropertyName;
                if (string.IsNullOrWhiteSpace(propertyName))
                    continue;

                // Add the current property
                yield return $"n.{propertyName} = \"{value?.AutoRedact(propertyName)}\"";
            }
        }

        private static bool IsSimpleType(Type type)
        {
            return type.IsPrimitive || type.IsEnum || type == typeof(string) || type == typeof(decimal);
        }

        public async Task<bool> CreateRelationshipsAsync<T>(IEnumerable<T> fromNodes) where T : GraphNode
        {
            await using var session = neo4jDriver.AsyncSession();
            foreach (var node in fromNodes)
            {
                var result = await CreateRelationshipsAsync(node, session).ConfigureAwait(false);
                if (!result)
                    return false; // exit on failure. (may want to continue on failure of individual nodes?)
            }

            return true;
        }

        public async Task<bool> CreateRelationshipsAsync<T>(T fromNode) where T : GraphNode
        {
            await using var session = neo4jDriver.AsyncSession();
            return await CreateRelationshipsAsync(fromNode, session);
        }

        public async Task<bool> CreateRelationshipsAsync<T>(T fromNode, IAsyncSession session) where T : GraphNode
        {
            var nodeType = fromNode.GetType();
            var properties = nodeType.GetProperties();

            foreach (var property in properties)
            {
                var relationshipAttribute = property.GetCustomAttributes()
                    .FirstOrDefault(attr => attr.GetType().IsGenericType && attr.GetType()
                        .GetGenericTypeDefinition() == typeof(NodeRelationshipAttribute<>));


                if (relationshipAttribute != null)
                {
                    var relatedNodeType = relationshipAttribute.GetType().GetGenericArguments()[0].Name;
                    //var relationshipType = relationshipAttribute.GetType().GetGenericArguments()[0];
                    var relationshipName = relationshipAttribute.GetType().GetProperty("RelationshipName")?.
                        GetValue(relationshipAttribute)?.ToString()?.ToUpper();

                    if (string.IsNullOrWhiteSpace(relationshipName))
                    {
                        logger.LogError("RelationshipName is null or empty {NodeType}.", nodeType.Name);
                        return false;
                    }

                    if (string.IsNullOrWhiteSpace(relatedNodeType))
                    {
                        logger.LogError("relatedNodeType is null or empty {NodeType}.", nodeType.Name);
                        return false;
                    }

                    if (property.GetValue(fromNode) is IEnumerable<string> relatedNodes)
                    {
                        foreach (var toNodeName in relatedNodes)
                        {
                            // relatedNode string indicates which GraphNode this node relates to
                            var toNode = dataSourceService.GetSourceNodeFor<GraphNode>(relatedNodeType, toNodeName);
                            if(toNode == null)
                            {
                                logger.LogError("toNode is null {NodeType} {toNodeName} (in a sub that is not loaded?)", relatedNodeType, toNodeName);
                                continue; // skip to next related node
                            }
                            logger.LogInformation("{from}-[{relationship}]->{to}", relationshipName, fromNode.DisplayName, toNode.DisplayName);

                            var query = 
 $$"""
 MATCH (from: {{fromNode.LabelName}} {{{fromNode.GetPrimaryKeyName()}}: "{{fromNode.GetPrimaryKeyValue()}}"})
 MATCH (to:   {{toNode.LabelName}} {{{toNode.GetPrimaryKeyName()}}: "{{toNode.GetPrimaryKeyValue()}}" })
 MERGE (from)-[rel:{{relationshipName}}]->(to)
 """;

                            var result = await ExecuteWriteQuery(session, query);
                            if (!result)
                                logger.LogWarning("Failed to create {relationship} {from} {to}", relationshipName, fromNode.DisplayName, toNode.DisplayName);
                        }
                    }
                }
            }

            return true;
        }

        private async Task<bool> ExecuteWriteQuery(IAsyncSession session, string query)
        {

            try
            {
                var result = await session.ExecuteWriteAsync(async tx =>
                {
                    await tx.RunAsync(query, new { Now });
                    return true;
                });
                if (!result)
                {
                    // only write to console in case there are secrets in the query
                    Console.WriteLine("**** write query failed ****{query}", query);
                }

                return result;
            }
            catch (AuthenticationException authEx)
            {
                logger.LogError(authEx, "UpsertNode auth error.");
                throw;
            }
        }

        public async Task<bool> EnforceUniqueConstraints(IEnumerable<INodeService> nodeServices)
        {
            await using var session = neo4jDriver.AsyncSession();
            foreach (var nodeService in nodeServices)
            {
                if(! nodeService.EnforceUniqueConstraint)
                    continue;

                var type = nodeService.GetType();

                // Get the base type (e.g FileNodeService<Movie>)
                var baseType = type.BaseType;
                if (baseType == null || !baseType.IsGenericType) 
                    continue;
                var genericType = baseType.GetGenericArguments()[0]; // Should be typeof(Movie)
                if (Activator.CreateInstance(genericType) is GraphNode instance)
                {
                    var query = GetUniqueConstraintCypher(instance);
                    await ExecuteWriteQuery(session, query);
                }
            }

            return true;
        }

        public async Task<NodeRelationshipsResponse> GetNodesAndRelationshipsAsync()
        {
            await using var session = neo4jDriver.AsyncSession();
            return await GetNodesAndRelationshipsAsync(session);
        }

        /// <summary>
        /// Get all node types and their relationships (in and out).
        /// </summary>
        /// <returns>useful if you want to feed your graph map into AI</returns>
        public async Task<NodeRelationshipsResponse> GetNodesAndRelationshipsAsync(IAsyncSession session)
        {
            // why exclude? I pass the result into AI to help it gen cypher. If a rel exists on many NodeType's, to minimize noise (and cost) I pass this instead: 
            // example: "GlobalOutgoingRelationships": ["IN_GROUP"]
            var excludedOutRelationships = config.GetSection("Neo4jLiteRepo:GetNodesAndRelationships:excludedOutRelationships")
                .Get<List<string>>() ?? [];
            var excludedInRelationships = config.GetSection("Neo4jLiteRepo:GetNodesAndRelationships:excludedInRelationships")
                .Get<List<string>>() ?? [];

            // Create a parameters dictionary
            var parameters = new Dictionary<string, object>
            {
                { "excludedOutRels", excludedOutRelationships },
                { "excludedInRels", excludedInRelationships }
            };

            var query = $$"""
                          CALL db.labels() YIELD label AS nodeType
                          WITH nodeType
                          ORDER BY nodeType
                          // Find outgoing relationships
                          OPTIONAL MATCH (n)-[r]->() 
                          WHERE nodeType IN labels(n)
                          WITH nodeType, collect(DISTINCT type(r)) AS allOutgoingRels
                          // Find incoming relationships
                          OPTIONAL MATCH (n)<-[r]-() 
                          WHERE nodeType IN labels(n)
                          WITH nodeType, allOutgoingRels, collect(DISTINCT type(r)) AS allIncomingRels
                          // Filter out excluded relationships
                          WITH 
                              nodeType,
                              [rel IN allOutgoingRels WHERE rel IS NOT NULL AND NOT rel IN $excludedOutRels ] AS outgoingRels,
                              [rel IN allIncomingRels WHERE rel IS NOT NULL AND NOT rel IN $excludedInRels] AS incomingRels
                          // Format results
                          RETURN 
                              nodeType AS NodeType,
                              outgoingRels AS OutgoingRelationships,
                              incomingRels AS IncomingRelationships
                          ORDER BY nodeType
                          """;

            var result = await session.RunAsync(query, parameters);
            var records = await result.ToListAsync();

            var response = new NodeRelationshipsResponse
            {
                QueriedAt = DateTime.UtcNow,
                NodeTypes = records.Select(record => new NodeRelationshipInfo
                {
                    NodeType = record["NodeType"].As<string>(),
                    OutgoingRelationships = record["OutgoingRelationships"].As<List<string>>(),
                    IncomingRelationships = record["IncomingRelationships"].As<List<string>>()
                }).ToList()
            };

            return response;
        }

        /// <summary>
        /// Enforces a unique constraint on a node
        /// </summary>
        private string GetUniqueConstraintCypher<T>(T node) where T : GraphNode
        {
            logger.LogInformation("CREATE UNIQUE CONSTRAINT {node}", node.LabelName);
            return $"""
                    CREATE CONSTRAINT {node.LabelName.ToLower()}_{node.GetPrimaryKeyName().ToLower()}_is_unique IF NOT EXISTS
                    FOR (n:{node.LabelName})
                    REQUIRE n.{node.GetPrimaryKeyName()} IS UNIQUE
                    """;
        }

        public async ValueTask DisposeAsync()
        {
            await neo4jDriver.DisposeAsync();
        }
    }


}