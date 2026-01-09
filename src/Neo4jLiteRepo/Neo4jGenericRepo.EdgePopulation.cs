using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Attributes;
using Neo4jLiteRepo.Helpers;
using System.Reflection;

namespace Neo4jLiteRepo;

/// <summary>
/// Edge object population operations - hydrates object collections from ID lists.
/// </summary>
public partial class Neo4jGenericRepo
{
    /// <summary>
    /// Hydrates object collections for a node that already has edge ID lists populated.
    /// Takes a node with List&lt;string&gt; edge IDs (e.g. Topics, RelatedSections) and loads the actual objects
    /// into corresponding collections (e.g. CoveredTopics, Sections).
    /// </summary>
    /// <typeparam name="T">Node type with relationship attributes</typeparam>
    /// <param name="node">Node instance with ID lists populated (from LoadAsync, LoadAllAsync, etc.)</param>
    /// <param name="edgesToLoad">Optional: specific edge property names to load. If null, loads all defined edges.</param>
    /// <param name="tx">Optional transaction</param>
    /// <param name="ct">Cancellation token</param>
    /// <remarks>
    /// This method introspects the node type for properties decorated with [NodeRelationship&lt;T&gt;] attributes,
    /// finds the corresponding List&lt;string&gt; ID properties, and batch-loads the related nodes to populate
    /// object collection properties.
    /// 
    /// For Article example:
    /// - Topics (List&lt;string&gt;) ? loads Topic objects into CoveredTopics (List&lt;Topic&gt;)
    /// - RelatedSections (List&lt;string&gt;) ? loads Section objects into Sections (List&lt;Section&gt;)
    /// 
    /// The method uses reflection to discover collection properties by naming convention:
    /// - ID property: "Topics" or "RelatedXxx"
    /// - Object property: Related type name pluralized (e.g., "CoveredTopics", "Sections")
    /// </remarks>
    public async Task PopulateEdgeObjectsAsync<T>(T node, IEnumerable<string>? edgesToLoad = null, IAsyncTransaction? tx = null, CancellationToken ct = default)
        where T : GraphNode
    {
        if (node == null) throw new ArgumentNullException(nameof(node));
        ct.ThrowIfCancellationRequested();

        var nodeType = typeof(T);
        var properties = nodeType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

        // Build filter set if specified
        var loadSet = edgesToLoad != null 
            ? new HashSet<string>(edgesToLoad, StringComparer.OrdinalIgnoreCase) 
            : null;

        // Find all properties with NodeRelationship attribute
        var relationshipProps = properties
            .Where(p => p.GetCustomAttributes()
                .Any(attr => attr.GetType().IsGenericType && 
                           attr.GetType().GetGenericTypeDefinition() == typeof(NodeRelationshipAttribute<>)))
            .ToList();

        foreach (var idListProperty in relationshipProps)
        {
            // Get the relationship attribute
            var relationshipAttribute = idListProperty.GetCustomAttributes()
                .First(attr => attr.GetType().IsGenericType && 
                             attr.GetType().GetGenericTypeDefinition() == typeof(NodeRelationshipAttribute<>));

            // Get the target type (e.g., Topic, Section)
            var targetType = relationshipAttribute.GetType().GetGenericArguments()[0];
            var targetTypeName = targetType.Name;

            // Apply filter if specified
            if (loadSet != null && !loadSet.Contains(idListProperty.Name, StringComparer.OrdinalIgnoreCase) &&
                !loadSet.Contains(targetTypeName, StringComparer.OrdinalIgnoreCase))
            {
                continue; // Skip this edge type
            }

            // Get the ID list from the node (e.g., Topics, RelatedSections)
            var idListValue = idListProperty.GetValue(node);
            if (idListValue is not IEnumerable<string> ids)
                continue;

            var idList = ids.Where(id => !string.IsNullOrWhiteSpace(id)).Distinct().ToList();
            if (idList.Count == 0)
                continue;

            // Find the target object collection property
            // Try multiple naming patterns:
            // 1. "Covered" + TypeName (e.g., CoveredTopics for Topics)
            // 2. TypeName plural (e.g., Sections for Section)
            // 3. Same name as ID property but with object type (e.g., Topics could hold Topic objects)
            PropertyInfo? targetCollectionProp = null;

            // Pattern 1: CoveredXxx
            var coveredName = "Covered" + targetTypeName + "s";
            targetCollectionProp = properties.FirstOrDefault(p => 
                string.Equals(p.Name, coveredName, StringComparison.OrdinalIgnoreCase) &&
                IsListOfType(p.PropertyType, targetType));

            // Pattern 2: Pluralized type name
            if (targetCollectionProp == null)
            {
                var pluralName = targetTypeName + "s";
                targetCollectionProp = properties.FirstOrDefault(p =>
                    string.Equals(p.Name, pluralName, StringComparison.OrdinalIgnoreCase) &&
                    IsListOfType(p.PropertyType, targetType));
            }

            // Pattern 3: Find any List<TargetType> property
            if (targetCollectionProp == null)
            {
                targetCollectionProp = properties.FirstOrDefault(p =>
                    p.Name != idListProperty.Name && // Don't use the ID property itself
                    IsListOfType(p.PropertyType, targetType));
            }

            if (targetCollectionProp == null)
            {
                _logger.LogWarning("Could not find object collection property for {IdProperty} (target type: {TargetType}) on {NodeType}",
                    idListProperty.Name, targetTypeName, nodeType.Name);
                continue;
            }

            // Batch load the related objects
            var loadedObjects = await BatchLoadNodesByIdsAsync(targetType, idList, tx, ct);

            // Set the collection property
            var typedList = ConvertToTypedList(loadedObjects, targetType);
            targetCollectionProp.SetValue(node, typedList);

            _logger.LogInformation("Populated {Count} {TargetType} objects for {NodeType}.{Property}",
                loadedObjects.Count, targetTypeName, nodeType.Name, targetCollectionProp.Name);
        }
    }

    /// <summary>
    /// Batch loads nodes of a specific type by their IDs.
    /// </summary>
    private async Task<List<GraphNode>> BatchLoadNodesByIdsAsync(Type targetType, List<string> ids, IAsyncTransaction? tx, CancellationToken ct)
    {
        // Create a temp instance to get label and PK name
        var tempInstance = Activator.CreateInstance(targetType) as GraphNode;
        if (tempInstance == null)
            throw new InvalidOperationException($"Cannot create instance of {targetType.Name}");

        var label = tempInstance.LabelName;
        var pkName = tempInstance.GetPrimaryKeyName();

        // Build query to load nodes by IDs
        var query = $$"""
            UNWIND $ids AS id
            MATCH (n:{{label}} { {{pkName}}: id })
            RETURN n
            """;

        var parameters = new Dictionary<string, object>
        {
            { "ids", ids }
        };

        async Task<List<GraphNode>> ExecAsync(IAsyncQueryRunner runner)
        {
            var cursor = await runner.RunAsync(query, parameters);
            var results = new List<GraphNode>();

            while (await cursor.FetchAsync())
            {
                var record = cursor.Current;
                if (!record.Keys.Contains("n")) continue;

                var node = record["n"].As<INode>();
                var obj = MapNodeToObject(node, targetType);
                results.Add(obj);
            }

            return results;
        }

        if (tx != null)
        {
            return await ExecAsync(tx);
        }

        await using var session = StartSession();
        return await session.ExecuteReadAsync(async rtx => await ExecAsync(rtx));
    }

    /// <summary>
    /// Maps a Neo4j node to an object of a specific type using reflection.
    /// Similar to MapNodeToObject&lt;T&gt; but works with Type parameter.
    /// </summary>
    private GraphNode MapNodeToObject(INode node, Type targetType)
    {
        var obj = Activator.CreateInstance(targetType) as GraphNode;
        if (obj == null)
            throw new InvalidOperationException($"Cannot create instance of {targetType.Name}");

        var properties = targetType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var nodeProps = GetNodePropertiesDictionary(node);

        foreach (var prop in properties.Where(p => p.SetMethod != null && p.SetMethod.IsPublic))
        {
            var attr = prop.GetCustomAttribute<NodePropertyAttribute>();
            List<string> candidates = [];

            if (!string.IsNullOrWhiteSpace(attr?.PropertyName))
                candidates.Add(attr!.PropertyName);

            candidates.Add(prop.Name.ToGraphPropertyCasing());
            if (!candidates.Contains(prop.Name))
                candidates.Add(prop.Name);

            foreach (var candidate in candidates.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                if (nodeProps.TryGetValue(candidate, out var value) && value != null)
                {
                    try
                    {
                        // Use existing conversion helpers
                        var convertedValue = ConvertValue(value, prop.PropertyType);
                        prop.SetValue(obj, convertedValue);
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "Failed to set property {Property} on {Type}", prop.Name, targetType.Name);
                    }
                }
            }
        }

        return obj;
    }

    /// <summary>
    /// Converts a value to the target type using existing ValueConversionExtensions.
    /// </summary>
    private static object? ConvertValue(object value, Type targetType)
    {
        if (targetType == typeof(float[]))
            return value.ConvertToFloatArray();
        if (targetType == typeof(Guid))
            return value.ConvertToGuid();
        if (targetType == typeof(List<string>))
            return value.ConvertToStringList();
        if (targetType == typeof(DateTimeOffset))
            return value.ConvertToDateTimeOffset();
        if (targetType == typeof(DateTime))
            return value.ConvertToDateTime();
        if (targetType.FullName == "Neo4jLiteRepo.Models.SequenceText")
            return value.ConvertToSequenceText();
        if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(List<>) &&
            targetType.GetGenericArguments()[0].FullName == "Neo4jLiteRepo.Models.SequenceText")
            return value.ConvertToSequenceTextList();

        return Convert.ChangeType(value, targetType);
    }

    /// <summary>
    /// Checks if a property type is List&lt;TargetType&gt;.
    /// </summary>
    private static bool IsListOfType(Type propertyType, Type targetType)
    {
        if (!propertyType.IsGenericType)
            return false;

        var genericDef = propertyType.GetGenericTypeDefinition();
        if (genericDef != typeof(List<>) && genericDef != typeof(IList<>) && genericDef != typeof(IEnumerable<>))
            return false;

        var elementType = propertyType.GetGenericArguments()[0];
        return elementType == targetType || elementType.IsAssignableFrom(targetType) || targetType.IsAssignableFrom(elementType);
    }

    /// <summary>
    /// Converts a list of GraphNode objects to a typed List&lt;T&gt;.
    /// </summary>
    private static object ConvertToTypedList(List<GraphNode> objects, Type elementType)
    {
        var listType = typeof(List<>).MakeGenericType(elementType);
        var typedList = (System.Collections.IList)Activator.CreateInstance(listType)!;

        foreach (var obj in objects)
        {
            typedList.Add(obj);
        }

        return typedList;
    }
}
