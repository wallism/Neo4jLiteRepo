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
        Task<bool> CreateVectorIndexForEmbeddings(IList<string>? labelNames = null, int dimensions = 3072);

        Task<bool> UpsertNodes<T>(IEnumerable<T> nodes) where T : GraphNode;
        Task<bool> UpsertNode<T>(T node) where T : GraphNode;
        Task<bool> UpsertNode<T>(T node, IAsyncSession session) where T : GraphNode;

        Task<bool> CreateRelationshipsAsync<T>(IEnumerable<T> fromNodes) where T : GraphNode;
        Task<bool> CreateRelationshipsAsync<T>(T nodes) where T : GraphNode;
        Task<bool> CreateRelationshipsAsync<T>(T nodes, IAsyncSession session) where T : GraphNode;

        Task<NodeRelationshipsResponse> GetNodesAndRelationshipsAsync();
        Task<NodeRelationshipsResponse> GetNodesAndRelationshipsAsync(IAsyncSession session);

        Task<IEnumerable<T>> ExecuteReadListAsync<T>(string query, string returnObjectKey, IDictionary<string, object>? parameters = null) where T : new();

        Task<IEnumerable<string>> ExecuteReadListStringsAsync(string query, string returnObjectKey, IDictionary<string, object>? parameters = null);
        
        Task<T> ExecuteReadScalarAsync<T>(string query, IDictionary<string, object>? parameters = null);        /// <summary>
        /// Executes a vector similarity search query to find relevant content chunks
        /// </summary>
        /// <param name="questionEmbedding">The embedding vector of the question</param>
        /// <param name="topK">Number of most relevant chunks to return</param>
        /// <param name="includeContext">Whether to include related chunks and parent context</param>
        /// <param name="similarityThreshold">Minimum cosine similarity threshold (0-1) for matching content</param>
        /// <returns>A list of strings containing the content and article information</returns>
        Task<List<string>> ExecuteVectorSimilaritySearchAsync(
            float[] questionEmbedding, 
            int topK = 5, 
            bool includeContext = true,
            double similarityThreshold = 0.6);
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


        private string GetNodeProperties<T>(T node, int depth = 5) where T : GraphNode
        {
            var declaredNodeProperties = GetNodePropertiesRecursive(node, depth);
            return string.Join(",\n  ", declaredNodeProperties);
        }

        private IEnumerable<string> GetNodePropertiesRecursive(object? obj, int depth)
        {
            if (obj == null || depth <= 0)
                yield break;

            var properties = obj.GetType().GetProperties()
                .Where(p => p.GetCustomAttribute<NodePropertyAttribute>() != null);

            foreach (var property in properties)
            {
                var value = property.GetValue(obj);
                if (value != null && !IsSimpleType(value.GetType()) && !IsEnumerable(value.GetType()))
                {
                    // If a property has [NodeProperty("")] then we need to recurse into child objects
                    foreach (var childProperty in GetNodePropertiesRecursive(value, depth - 1))
                    {
                        yield return childProperty;
                    }
                    continue;
                }

                var attribute = property.GetCustomAttribute<NodePropertyAttribute>();
                if (attribute == null)
                    continue;
                if (attribute.Exclude)
                    continue;

                var propertyName = attribute.PropertyName;

                if (value == null)
                {
                    if (!string.IsNullOrWhiteSpace(attribute.StringNullDefault))
                    {
                        yield return $"n.{propertyName} = \"{attribute.StringNullDefault}\"";
                        continue;
                    }

                    if (attribute is BoolNodePropertyAttribute boolAttribute)
                    {
                        yield return $"n.{propertyName} = {boolAttribute.BoolNullDefault.ToString()}";
                        continue;
                    }


                    // note: if the property has a null value, it won't be returned when a query is run against Neo4j
                    logger.LogInformation("{propertyName} value is null (won't be surfaced by neo4j)", propertyName);
                    yield return $"n.{propertyName} = null";
                    continue;
                }

                if (value.IsBool())
                {
                    yield return $"n.{propertyName} = {value}";
                    continue;
                }

                if (value is IEnumerable<string> enumerable)
                {
                    var stringList = enumerable.Select(x => $"\"{x.SanitizeForCypher()}\"");
                    yield return $"n.{propertyName} = [{string.Join(", ", stringList)}]";
                    continue;
                }

                if (value is DateTime dateTime)
                {
                    // Format to ISO 8601 format that Neo4j expects
                    var formattedDate = dateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
                    yield return $"n.{propertyName} = datetime('{formattedDate}')";
                    continue;
                }

                if (value is float[] floatArray)
                {
                    // Format the float array without quotes, just comma-separated values
                    var vectorString = string.Join(", ", floatArray);
                    yield return $"n.{propertyName} = [{vectorString}]";
                    continue;
                }

                //if (IsEnumerable(value.GetType())) todo: handle custom types
                //{
                //    yield return $"n.{propertyName} = [{string.Join(value)}]";
                //    continue;
                //}

                // Add the current property (as string)
                yield return $"n.{propertyName} = \"{value.SanitizeForCypher().AutoRedact(propertyName)}\"";
            }
        }


        private static bool IsSimpleType(Type type)
        {
            return type.IsPrimitive || type.IsEnum || type == typeof(string) || type == typeof(decimal);
        }

        static bool IsEnumerable(Type type)
        {
            return typeof(System.Collections.IEnumerable).IsAssignableFrom(type)
                   && type != typeof(string);
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
                    var relatedNodeType = relationshipAttribute.GetType().GetGenericArguments()[0];
                    var relatedNodeTypeName = relatedNodeType.Name;
                    //var relationshipType = relationshipAttribute.GetType().GetGenericArguments()[0];
                    var relationshipName = relationshipAttribute.GetType().GetProperty("RelationshipName")?.
                        GetValue(relationshipAttribute)?.ToString()?.ToUpper();

                    if (string.IsNullOrWhiteSpace(relationshipName))
                    {
                        logger.LogError("RelationshipName is null or empty {NodeType}.", nodeType.Name);
                        return false;
                    }

                    if (string.IsNullOrWhiteSpace(relatedNodeTypeName))
                    {
                        logger.LogError("relatedNodeType is null or empty {NodeType}.", nodeType.Name);
                        return false;
                    }

                    if (property.GetValue(fromNode) is IEnumerable<string> relatedNodes)
                    {
                        foreach (var toNodeId in relatedNodes)
                        {
                            // relatedNode string indicates which GraphNode this node relates to
                            var toNode = dataSourceService.GetSourceNodeFor<GraphNode>(relatedNodeTypeName, toNodeId);
                            if (toNode == null)
                            {
                                // Try to load the node from the graph DB if not found in memory
                                // We need to get the primary key property name for the related node type
                                var pkName = fromNode.GetPrimaryKeyName(); // fallback if not available for relatedNodeType
                                if (Activator.CreateInstance(relatedNodeType) is GraphNode tempInstance)
                                    pkName = tempInstance.GetPrimaryKeyName();

                                // Properly escape curly braces in interpolated string
                                var toNodeQuery = $"MATCH (n:{relatedNodeTypeName} {{ {pkName}: '{toNodeId}' }}) RETURN n LIMIT 1";
                                
                                // Use reflection to call ExecuteReadListAsync with the proper generic type parameter
                                var executeMethod = typeof(Neo4jGenericRepo).GetMethod(nameof(ExecuteReadListAsync), new[] { typeof(string), typeof(string), typeof(IDictionary<string, object>) })
                                    ?.MakeGenericMethod(relatedNodeType);
                                
                                if (executeMethod == null)
                                {
                                    logger.LogError("Failed to create generic method for ExecuteReadListAsync with type {relatedNodeType}", relatedNodeTypeName);
                                    continue;
                                }                                // Create empty dictionary for parameters to avoid null
                                var emptyParams = new Dictionary<string, object>();
                                
                                try 
                                {
                                    // Directly invoke and await the task
                                    var taskObj = executeMethod.Invoke(this, [toNodeQuery, "n", emptyParams]);
                                    
                                    if (taskObj is Task resultTask)
                                    {
                                        // Wait for the task to complete
                                        await resultTask.ConfigureAwait(false);
                                        
                                        // Get the result using reflection
                                        var resultProperty = resultTask.GetType().GetProperty("Result");
                                        if (resultProperty != null)
                                        {
                                            var queryResult = resultProperty.GetValue(resultTask);
                                            
                                            // Process results if they exist
                                            if (queryResult is IEnumerable<object> objectResults)
                                            {
                                                var nodeObj = objectResults.FirstOrDefault();
                                                if (nodeObj is GraphNode graphNode)
                                                {
                                                    toNode = graphNode;
                                                }
                                            }
                                            else if (queryResult is System.Collections.IEnumerable enumerable)
                                            {
                                                // Handle non-generic IEnumerable
                                                var enumerator = enumerable.GetEnumerator();
                                                if (enumerator.MoveNext() && enumerator.Current is GraphNode foundNode)
                                                {
                                                    toNode = foundNode;
                                                }
                                            }
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    logger.LogError(ex, "Error executing query for node type {relatedNodeType}", relatedNodeTypeName);
                                }
                                if (toNode == null)
                                {
                                    logger.LogError("toNode is null {NodeType} {toNodeName} (not found in memory or DB)", relatedNodeTypeName, toNodeId);
                                    continue; // skip to next related node
                                }
                            }
                            logger.LogInformation("{from}-[{relationship}]->{to}", fromNode.DisplayName, relationshipName, toNode.DisplayName);

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
                if (!nodeService.EnforceUniqueConstraint)
                    continue;

                var type = nodeService.GetType();

                // Get the base type (e.g FileNodeService<Movie>)
                var baseType = type.BaseType;
                if (baseType == null || !baseType.IsGenericType)
                    continue;
                var genericType = baseType.GetGenericArguments()[0]; // e.g. typeof(Movie)
                if (Activator.CreateInstance(genericType) is GraphNode instance)
                {
                    var query = GetUniqueConstraintCypher(instance);
                    await ExecuteWriteQuery(session, query);
                }
            }

            return true;
        }

        /// <summary>
        /// Allows for multiple nodes having embeddings and vector indexes,
        /// however one is usually better when searching for semantic meaning across all data.
        /// </summary>
        /// <remarks>the nodes must have an "embedding" property.
        /// note: defaults to 3072 dimensions (for text-embedding-3-large).</remarks>
        public async Task<bool> CreateVectorIndexForEmbeddings(IList<string>? labelNames = null, int dimensions = 3072)
        {
            if (labelNames is null || !labelNames.Any())
                return true;

            /*
             * text-embedding-3-large | 3072 dimensions
             * text-embedding-ada-002 | 1536 dimensions
             * Important: If the embedding model changes, the index MUST be dropped and rebuilt!
             *
             * todo: auto set the dimensions based on the embedding model (used in AI layer)
             */

            await using var session = neo4jDriver.AsyncSession();
            foreach (var labelName in labelNames)
            {
                var query = $$"""
                               CREATE VECTOR INDEX {{labelName.ToLower()}}_chunk_embedding IF NOT EXISTS
                               FOR (c:{{labelName}}) 
                               ON c.embedding
                               OPTIONS {
                                indexConfig: {
                                   `vector.dimensions`: {{dimensions}},
                                   `vector.similarity_function`: 'cosine'
                                } 
                               }
                               """;
                await ExecuteWriteQuery(session, query);
            }

            return true;
        }

        public async Task<IEnumerable<string>> ExecuteReadListStringsAsync(string query, string returnObjectKey, IDictionary<string, object>? parameters = null)
        {
            await using var session = neo4jDriver.AsyncSession();
            try
            {
                parameters ??= new Dictionary<string, object>();

                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var res = await tx.RunAsync(query, parameters);
                    var records = await res.ToListAsync();

                    // Assuming the returned value is a list of objects (like ["a", "b", "c"])
                    var list = records
                        .SelectMany(x => x[returnObjectKey].As<List<string>>())
                        .Distinct()
                        .ToList();

                    return list;
                });

                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Problem executing {query}", query);
                throw;
            }
        }

        public async Task<IEnumerable<T>> ExecuteReadListAsync<T>(string query,
            string returnObjectKey, IDictionary<string, object>? parameters = null) where T : new()
        {
            await using var session = neo4jDriver.AsyncSession();
            try
            {
                parameters ??= new Dictionary<string, object>();

                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var res = await tx.RunAsync(query, parameters);
                    var records = await res.ToListAsync();

                    // Assuming the returned value is a list of objects (like ["a", "b", "c"])
                    var list = records
                        .Select(record => MapNodeToObject<T>(record[returnObjectKey].As<INode>()))
                        .Distinct()
                        .ToList();

                    return list;
                });

                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Problem executing {query}", query);
                throw;
            }
        }

        private T MapNodeToObject<T>(INode node) where T : new()
        {
            var obj = new T();
            var tProperties = typeof(T).GetProperties()
                .Where(p => p.SetMethod != null && p.SetMethod.IsPublic)
                .ToArray();

            foreach (var tProp in tProperties)
            {
                var tPropertyName = tProp.Name.ToGraphPropertyCasing();
                if (node.Properties.TryGetValue(tPropertyName, out var value))
                {
                    if (value == null)
                        continue;

                    // Handle float[] (Single[]) property type
                    if (tProp.PropertyType == typeof(float[]))
                    {
                        if (value is IEnumerable<object> objArray)
                        {
                            var floatArray = objArray.Select(Convert.ToSingle).ToArray();
                            tProp.SetValue(obj, floatArray);
                        }
                        else if (value is float[] floatArray)
                        {
                            tProp.SetValue(obj, floatArray);
                        }
                        else if (value is double[] doubleArray)
                        {
                            tProp.SetValue(obj, doubleArray.Select(d => (float)d).ToArray());
                        }
                        continue;
                    }

                    if (tProp.PropertyType == typeof(Guid))
                    {
                        if (value is string stringValue && Guid.TryParse(stringValue, out var guidValue))
                        {
                            tProp.SetValue(obj, guidValue);
                        }
                        continue;
                    }

                    // Handle type conversion for other types
                    var convertedValue = Convert.ChangeType(value, tProp.PropertyType);
                    tProp.SetValue(obj, convertedValue);
                }
            }

            return obj;
        }

        /// <summary>
        /// Execute read scalar as an asynchronous operation.
        /// </summary>
        /// <remarks>untested - 20250424</remarks>
        public async Task<T> ExecuteReadScalarAsync<T>(string query, IDictionary<string, object>? parameters = null)
        {
            await using var session = neo4jDriver.AsyncSession();
            try
            {
                parameters = parameters ?? new Dictionary<string, object>();

                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var res = await tx.RunAsync(query, parameters);
                    var scalar = (await res.SingleAsync())[0].As<T>();
                    return scalar;
                });

                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Problem executing {query}", query);
                throw;
            }
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
        
        /// <summary>
        /// Executes a vector similarity search query to find relevant content chunks
        /// </summary>
        /// <param name="questionEmbedding">The embedding vector of the question</param>
        /// <param name="topK">Number of most relevant chunks to return</param>
        /// <param name="includeContext">Whether to include related chunks and parent context</param>
        /// <param name="similarityThreshold">Minimum cosine similarity threshold (0-1) for matching content. 
        /// Higher values (e.g. 0.8) will return only very close matches and may result in fewer results.
        /// Lower values (e.g. 0.5) will return more results but may include less relevant content.
        /// Values between 0.6-0.75 are typically a good starting point.</param>
        /// <returns>A list of strings containing the content and article information</returns>
        public async Task<List<string>> ExecuteVectorSimilaritySearchAsync(
            float[] questionEmbedding, 
            int topK = 20, 
            bool includeContext = true,
            double similarityThreshold = 0.7)
        {
            var query = await GetCypherFromFile("ExecuteVectorSimilaritySearch.cypher", logger);

            await using var session = neo4jDriver.AsyncSession();
            var result = await session.ExecuteReadAsync(async tx =>
            {               
                // Run the query
                var cursor = await tx.RunAsync(query, new
                {
                    questionEmbedding,
                    topK
                });

                // Process results
                var resultsDict = new Dictionary<string, Dictionary<string, object>>();
                
                await foreach (var record in cursor)
                {
                    var id = record["id"].As<string>();
                    
                    // If we've already seen this chunk, skip it (avoid duplicates)
                    if (resultsDict.ContainsKey(id))
                        continue;
                    
                    resultsDict[id] = new Dictionary<string, object>
                    {
                        ["id"] = id,
                        ["content"] = record["content"].As<string>(),
                        ["articleTitle"] = record["articleTitle"].As<string>(),
                        ["articleUrl"] = record["articleUrl"].As<string>(),
                        ["score"] = record["score"].As<double>(),
                        ["entities"] = record["entities"].As<List<string>>(),
                        ["sequenceOrder"] = record["sequenceOrder"].As<int>(),
                        ["resultType"] = record["resultType"].As<string>()
                    };
                }
                
                // Sort by article title and sequence order for better readability
                var sortedResults = resultsDict.Values
                    .OrderBy(r => r["articleTitle"].ToString())
                    .ThenBy(r => (int)r["sequenceOrder"])
                    .ToList();
                
                // Format results to return
                var formattedResults = new List<string>();

                var currentArticle = "";
                foreach (var result in sortedResults)
                {
                    var articleTitle = result["articleTitle"]?.ToString() ?? "Unknown Article";
                    var articleUrl = result["articleUrl"]?.ToString() ?? "no-link";
                    
                    // If we're starting a new article, add a header
                    if (articleTitle != currentArticle)
                    {
                        if (formattedResults.Count > 0)
                            formattedResults.Add($"-- end article: {currentArticle} --");  // Add a clear delimiter between articles for LLM context
                            
                        formattedResults.Add($"-- start article: {articleTitle} --");
                        formattedResults.Add($"article url: {articleUrl}");
                        currentArticle = articleTitle;
                    }
                      
                    // Add content with prefix based on result type
                    var prefix = "";
                    var resultType = result["resultType"]?.ToString() ?? "unknown";
                    
                    if (resultType == "main")
                        prefix = "▶️ ";  // Highlight the main matches
                    else if (resultType == "next")
                        prefix = "⏩ ";  // Context that follows
                    else if (resultType == "previous") 
                        prefix = "⏪ ";  // Context that precedes
                    else if (resultType == "sibling")
                        prefix = "🔄 ";  // Related content
                    else if (resultType == "section_related")
                        prefix = "📑 ";  // Section-related content
                    else if (resultType == "subsection_related")
                        prefix = "📋 ";  // SubSection-related content
                        
                    var entities = result["entities"] as List<string> ?? new List<string>();
                    var entityInfo = entities.Any() ? $" [Entities: {string.Join(", ", entities)}]" : "";

                    var content = $"{prefix}{result["content"]}{entityInfo}";
                    if(!formattedResults.Contains(content) && !string.IsNullOrWhiteSpace(content))
                        formattedResults.Add(content);
                }
                
                return formattedResults;
            });

            return result;
        }

        /// <summary>
        /// Gets a Cypher query from a .cypher file in the Queries directory
        /// </summary>
        /// <param name="fileName">The name of the .cypher file without path</param>
        /// <param name="logger">Logger instance for logging messages</param>
        /// <returns>The contents of the Cypher query file as a string</returns>
        /// <exception cref="FileNotFoundException">Thrown when the query file cannot be found</exception>
        private static async Task<string> GetCypherFromFile(string fileName, ILogger<Neo4jGenericRepo> logger)
        {
            // Read the Cypher query from file
            var queryFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Queries", fileName);
            
            // If file doesn't exist at runtime location, try to find it relative to the source code
            if (!File.Exists(queryFilePath))
            {
                var projectPath = Path.GetDirectoryName(typeof(Neo4jGenericRepo).Assembly.Location);
                while (projectPath != null && !Directory.Exists(Path.Combine(projectPath, "Queries")))
                {
                    projectPath = Directory.GetParent(projectPath)?.FullName;
                }
                
                if (projectPath != null)
                {
                    queryFilePath = Path.Combine(projectPath, "Queries", fileName);
                }
            }
            
            // Load query from file
            if (File.Exists(queryFilePath))
            {
                var query = await File.ReadAllTextAsync(queryFilePath);
                logger.LogDebug("Loaded Cypher query from file: {FilePath} {length}", queryFilePath, query.Length);
                if(query.Length == 0)
                    throw new FileNotFoundException("Cypher query file is empty.", queryFilePath);
                return query;
            }
            else
            {
                // Log error and throw exception
                logger.LogError("Could not find Cypher query file at {FilePath}", queryFilePath);
                throw new FileNotFoundException("Cypher query file not found.", queryFilePath);
            }
        }

    }
}