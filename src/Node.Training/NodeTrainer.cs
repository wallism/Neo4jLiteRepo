using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Neo4jLiteRepo.Helpers;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Node.Training
{
    public class NodeTrainer(IConfiguration configuration, ILogger<NodeTrainer> logger)
    {
        private readonly List<AlwaysSeparateNodeProperty> alwaysSeparateNodeProps = configuration.
            GetSection("Neo4jLiteRepo:AlwaysSeparateNodeProps").Get<List<AlwaysSeparateNodeProperty>>() ?? [];
        private readonly List<string> alwaysIncludeProperties = configuration.GetSection("Neo4jLiteRepo:AlwaysIncludeProperty").Get<List<string>>() ?? [];
        private readonly List<string> alwaysExcludeProperties = configuration.GetSection("Neo4jLiteRepo:AlwaysExcludeProperty").Get<List<string>>() ?? [];
        private readonly List<string> dontIncludeClassNameInNodePropertyName = configuration.GetSection("Neo4jLiteRepo:DontIncludeClassNameInNodePropertyName").Get<List<string>>() ?? [];

        public async Task TrainFromJsonFiles()
        {
            var trainingDirectory = configuration["Neo4jLiteRepo:TrainingInputDirectory"];
            logger.LogInformation("Starting training from JSON files in {Directory}", trainingDirectory);
            
            // Ensure directory exists
            if (!Directory.Exists(trainingDirectory))
            {
                logger.LogError("Training directory {Directory} does not exist", trainingDirectory);
                return;
            }

            var jsonFiles = Directory.GetFiles(trainingDirectory, "*.json");
            logger.LogInformation("Found {Count} JSON files", jsonFiles.Length);

            foreach (var file in jsonFiles)
            {
                await ProcessJsonFile(file);
            }
        }

        /// <summary>
        /// Process a single JSON file to generate node classes and services.
        /// </summary>
        private async Task ProcessJsonFile(string filePath)
        {
            logger.LogInformation("Training JSON file: {File}", filePath);
            
            try
            {
                var json = await File.ReadAllTextAsync(filePath);
                var fileName = Path.GetFileNameWithoutExtension(filePath);
                var nodeTypeName = fileName.ToPascalCase();
                
                // Parse JSON to determine structure
                var jsonData = JsonConvert.DeserializeObject<dynamic>(json);

                // Check if it's an array at the root
                if (jsonData is JArray array)
                {
                    if (array.Count > 0)
                    {
                        // Use the first item to generate the structure
                        await GenerateNodeClass(nodeTypeName, array);
                        await GenerateNodeService(nodeTypeName);
                    }
                    else
                    {
                        logger.LogWarning("Empty array in {File}, cannot determine structure", filePath);
                    }
                }
                // If not, check if it's an object with a property that is an array (e.g., 'value')
                else if (jsonData is JObject obj)
                {
                    // Look for the first property that is a JArray
                    var arrayProp = obj.Properties().FirstOrDefault(p => p.Value is JArray);
                    if (arrayProp != null && arrayProp.Value is JArray nestedArray)
                    {
                        if (nestedArray.Count > 0)
                        {
                            await GenerateNodeClass(nodeTypeName, nestedArray);
                            await GenerateNodeService(nodeTypeName);
                        }
                        else
                        {
                            logger.LogWarning($"Array property '{arrayProp.Name}' in {{File}} is empty, cannot determine structure", filePath);
                        }
                    }
                    else
                    {
                        // Fallback: treat the object itself as the sample
                        await GenerateNodeClass(nodeTypeName, obj);
                        await GenerateNodeService(nodeTypeName);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing file {File}", filePath);
            }
        }

        private List<string> nestedClassNames = [];
        private List<string> nestedClasses = [];

        /// <summary>
        /// Generate a node class based on the JSON structure.
        /// </summary>
        /// <param name="nodeTypeName"></param>
        /// <param name="sampleData"></param>
        /// <returns></returns>
        private async Task GenerateNodeClass(string nodeTypeName, JToken sampleData)
        {
            logger.LogInformation("Generating node class: {NodeType}", nodeTypeName);
            var properties = new List<NodePropertyInfo>();
            var primaryKeyFound = false;
            // Step 1: Collect all nested class names
            nestedClassNames = GetNestedClassNames(sampleData);
            // Step 2: Generate all nested class code (after names are known)
            GenerateAllNestedClassCode(sampleData);

            var firstItem = sampleData[0];

            // Analyze properties
            if (firstItem is JObject sampleObj)
            {
                foreach (JProperty prop in sampleObj.Properties())
                {
                    if (!ShouldIncludeProperty(prop, (JArray)sampleData))
                        continue; // The property is always null or empty - exclude

                    var propName = prop.Name;
                    var propType = DeterminePropertyType(prop.Value ?? JValue.CreateNull(), propName);
                    var isArray = prop.Value != null && prop.Value.Type == JTokenType.Array;

                    // is a "primary key"?
                    var isPrimaryKey = !primaryKeyFound &&
                                      (propName.Equals("id", StringComparison.OrdinalIgnoreCase) ||
                                       propName.Equals($"{nodeTypeName}Id", StringComparison.OrdinalIgnoreCase) ||
                                       propName.EndsWith("Id", StringComparison.OrdinalIgnoreCase));

                    // Use config-driven AlwaysSeparateNodeProps for relationship detection
                    var separateNode = alwaysSeparateNodeProps.FirstOrDefault(p => p.Name.Equals(propName, StringComparison.InvariantCultureIgnoreCase));
                    var isRelationship = separateNode != null;

                    var relatedType = "";
                    if (isRelationship)
                    {
                        relatedType = propName.ToPascalCase();
                        // todo: de-pluralize the name
                        // For relationships, property type should be IEnumerable<string>
                        propType = "IEnumerable<string>";
                    }
                    else if (isArray && (
                        propName.EndsWith("Ids", StringComparison.OrdinalIgnoreCase) ||
                        propName.EndsWith("Keys", StringComparison.OrdinalIgnoreCase)))
                    {
                        relatedType = propName.EndsWith("Ids", StringComparison.OrdinalIgnoreCase)
                            ? propName.Substring(0, propName.Length - 3)
                            : propName.EndsWith("Keys", StringComparison.OrdinalIgnoreCase)
                                ? propName.Substring(0, propName.Length - 4)
                                : propName;
                        relatedType = relatedType.ToPascalCase();
                    }

                    if (isPrimaryKey) primaryKeyFound = true;

                    properties.Add(new NodePropertyInfo(propName, propType, isPrimaryKey, relatedType, separateNode));
                }
            }
            else
            {
                logger.LogWarning("Sample data is not a JObject for {NodeType}", nodeTypeName);
                return;
            }

            // If no primary key was found, use the first property
            if (!primaryKeyFound && properties.Count > 0)
            {
                logger.LogWarning("No primary key found for {NodeType}, using first property: {Property}",
                    nodeTypeName, properties[0].Name);
                var firstProp = properties[0];
                var separateNode = alwaysSeparateNodeProps.FirstOrDefault(p => p.Name.Equals(firstProp.Name, StringComparison.InvariantCultureIgnoreCase));
                properties[0] = new NodePropertyInfo(firstProp.Name, firstProp.Type, true, firstProp.RelatedType, separateNode);
            }

            // Generate the class code
            var code = GenerateNodeClassCode(nodeTypeName, properties);
            // Save to file using shared method
            await SaveGeneratedFile(Path.Combine("Nodes"), $"{nodeTypeName}.cs", code);
        }

        /// <summary>
        /// Returns a list of all nested class names found in the sample data.
        /// </summary>
        private List<string> GetNestedClassNames(JToken sampleData)
        {
            var names = new List<string>();
            if (sampleData is JArray arr && arr.Count > 0 && arr[0] is JObject sampleObj)
            {
                foreach (JProperty prop in sampleObj.Properties())
                {
                    AddNestedClassNamesRecursive(prop.Value, prop.Name.ToPascalCase(), names);
                }
            }
            return names;
        }

        /// <summary>
        /// Recursively adds nested class names for object and array-of-object properties.
        /// </summary>
        private void AddNestedClassNamesRecursive(JToken token, string propName, List<string> names)
        {
            if (token.Type == JTokenType.Array)
            {
                if (token is not JArray arr || !arr.HasValues)
                    return;
                var firstItem = arr[0];
                if (firstItem.Type == JTokenType.Object)
                {
                    var nestedTypeName = propName + "Item";
                    if (!names.Contains(nestedTypeName))
                        names.Add(nestedTypeName);
                    foreach (var nestedProp in ((JObject)firstItem).Properties())
                    {
                        AddNestedClassNamesRecursive(nestedProp.Value, nestedProp.Name.ToPascalCase(), names);
                    }
                }
            }
            else if (token.Type == JTokenType.Object)
            {
                var nestedTypeName = propName;
                if (!names.Contains(nestedTypeName))
                    names.Add(nestedTypeName);
                foreach (var nestedProp in ((JObject)token).Properties())
                {
                    AddNestedClassNamesRecursive(nestedProp.Value, nestedProp.Name.ToPascalCase(), names);
                }
            }
        }

        /// <summary>
        /// Generates all nested class code for the previously collected nested class names.
        /// </summary>
        private void GenerateAllNestedClassCode(JToken sampleData)
        {
            nestedClasses = [];
            if (sampleData is JArray arr && arr.Count > 0 && arr[0] is JObject sampleObj)
            {
                foreach (var className in nestedClassNames)
                {
                    // Find a sample object for this class name
                    var obj = FindSampleObjectForClassName(arr, className);
                    if (obj != null)
                    {
                        var classCode = GenerateNestedClass(className, obj);
                        nestedClasses.Add(classCode);
                    }
                }
            }
        }

        /// <summary>
        /// Finds a JObject in the sample data that matches the given class name.
        /// </summary>
        private JObject? FindSampleObjectForClassName(JArray arr, string className)
        {
            foreach (var item in arr)
            {
                if (item is JObject obj)
                {
                    var found = FindObjectRecursive(obj, className);
                    if (found != null)
                        return found;
                }
            }
            return null;
        }

        private JObject? FindObjectRecursive(JObject obj, string className)
        {
            foreach (var prop in obj.Properties())
            {
                if (prop.Value.Type == JTokenType.Object && prop.Name.ToPascalCase() == className)
                    return (JObject)prop.Value;
                if (prop.Value.Type == JTokenType.Array && prop.Name.ToPascalCase() + "Item" == className)
                {
                    var arr = (JArray)prop.Value;
                    if (arr.Count > 0 && arr[0] is JObject arrObj)
                        return arrObj;
                }
                // Recurse
                if (prop.Value.Type == JTokenType.Object)
                {
                    var found = FindObjectRecursive((JObject)prop.Value, className);
                    if (found != null)
                        return found;
                }
                else if (prop.Value.Type == JTokenType.Array)
                {
                    var arr = (JArray)prop.Value;
                    foreach (var arrItem in arr)
                    {
                        if (arrItem is JObject arrObj)
                        {
                            var found = FindObjectRecursive(arrObj, className);
                            if (found != null)
                                return found;
                        }
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Generate the C# code for the node class, including nested classes for object properties.
        /// </summary>
        /// <param name="nodeTypeName"></param>
        /// <param name="properties"></param>
        /// <returns></returns>
        private string GenerateNodeClassCode(string nodeTypeName, List<NodePropertyInfo> properties)
        {
            var sb = new StringBuilder();
            sb.AppendLine($@"using System;
using System.Collections.Generic;
using Neo4jLiteRepo;
using Neo4jLiteRepo.Attributes;

namespace Node.Training.Generated.Nodes
{{
    public class {nodeTypeName} : GraphNode
    {{
");

            // Add properties
            foreach (var prop in properties)
            {
                var pascalPropName = prop.Name.ToPascalCase();
                if (prop.IsPrimaryKey)
                {
                    sb.AppendLine("        [NodePrimaryKey]");
                    if(prop.Name.Equals("Id", StringComparison.InvariantCultureIgnoreCase))
                        sb.AppendLine($"        public override required {prop.Type} {pascalPropName} {{ get; set; }}");
                    else
                        sb.AppendLine($"        public {prop.Type} {pascalPropName} {{ get; set; }}");
                    sb.AppendLine();
                    continue;
                }
                
                if (prop.IsSeparateNode)
                {
                    var nestedClassName = prop.SeparateNode!.ClassName ?? prop.RelatedType;
                    sb.AppendLine($"        [NodeRelationship<{nestedClassName}>(\"HAS_{prop.RelatedType.ToUpperInvariant()}\")]");
                }
                else
                {
                    // If the property type is a nested class, use [NodeProperty("")]
                    if (nestedClassNames.Contains(prop.Type))
                        sb.AppendLine($"        [NodeProperty(\"\")]");
                    else
                        sb.AppendLine($"        [NodeProperty(nameof({pascalPropName}))]");
                }
                sb.AppendLine($"        public {prop.Type} {pascalPropName} {{ get; set; }}");
                sb.AppendLine();
            }
            // Add BuildDisplayName method
            var displayNameProp = properties.FirstOrDefault(p =>
                p.Name.Equals("Name", StringComparison.OrdinalIgnoreCase) ||
                p.Name.Equals("Title", StringComparison.OrdinalIgnoreCase))
                ?? properties.FirstOrDefault(p => p.IsPrimaryKey);

            var displayNamePropName = displayNameProp != null ? displayNameProp.Name : "Id";

            displayNamePropName = displayNamePropName.ToPascalCase();
            var pkProp = properties.FirstOrDefault(p => p.IsPrimaryKey);
            var pkPropName = pkProp != null ? pkProp.Name : "Id";
            pkPropName = pkPropName.ToPascalCase();
            sb.AppendLine($$"""        public override string BuildDisplayName() => {{displayNamePropName}} ?? $"{{{pkPropName}}}";""");
            // close the class
            sb.AppendLine("    }");

            // Add nested classes after the main node class
            foreach (var nested in nestedClasses.OrderDescending())
            {
                sb.AppendLine(nested);
            }
            sb.AppendLine("}");
            return sb.ToString();
        }

        /// <summary>
        /// Generate a node service class based on the JSON file path.
        /// </summary>
        /// <param name="nodeTypeName"></param>
        /// <returns></returns>
        private async Task GenerateNodeService(string nodeTypeName)
        {
            logger.LogInformation("Generating node service for: {NodeType}", nodeTypeName);
            
            var code = GenerateNodeServiceCode(nodeTypeName);
            // Save to file using shared method
            await SaveGeneratedFile(Path.Combine("NodeServices"), $"{nodeTypeName}NodeService.cs", code);
        }

        /// <summary>
        /// Generate the C# code for the node service.
        /// </summary>
        /// <param name="nodeTypeName"></param>
        /// <returns></returns>
        private string GenerateNodeServiceCode(string nodeTypeName)
        {
            // Generate code to set relationship arrays in RefreshNodeRelationships
            var relProps = alwaysSeparateNodeProps.Select(p => p.Name.ToPascalCase()).ToList();
            var relAssignments = new StringBuilder();
            relAssignments.AppendLine("            foreach (var node in data.Cast<" + nodeTypeName + ">())");
            relAssignments.AppendLine("            {");
            foreach (var relProp in relProps)
            {
                relAssignments.AppendLine($"                node.{relProp} = []; // TODO: Populate with related node keys");
            }
            relAssignments.AppendLine("            }");

            var code = $@"using Microsoft.Extensions.Configuration;
using Neo4jLiteRepo;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.NodeServices;
using Node.Training.Generated.Nodes;

namespace Node.Training.Generated.NodeServices
{{
    public class {nodeTypeName}NodeService(IConfiguration config, IDataRefreshPolicy dataRefreshPolicy) : FileNodeService<{nodeTypeName}>(config, dataRefreshPolicy)
    {{

        public override Task<IList<GraphNode>> RefreshNodeData(bool saveToFile = true) => Task.FromResult<IList<GraphNode>>([]);
        
        public override Task<IEnumerable<GraphNode>> LoadDataFromSource()
        {{
            // source data is static json files for the sample
            throw new NotImplementedException();
        }}

        public override Task<bool> RefreshNodeRelationships(IEnumerable<GraphNode> data)
        {{
{relAssignments.ToString()}            return Task.FromResult(true);
        }}
    }}
}}
";

            return code;
        }

        /// <summary>
        /// Maps a JTokenType to its corresponding C# type as a string.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private string GetCSharpTypeForJTokenType(JTokenType type)
        {
            return type switch
            {
                JTokenType.Integer => "int",
                JTokenType.Float => "double",
                JTokenType.String => "string",
                JTokenType.Boolean => "bool",
                JTokenType.Date => "DateTime",
                JTokenType.Object => "dynamic",
                _ => "string"
            };
        }

        /// <summary>
        /// Determine the C# type for a JSON property based on its JTokenType.
        /// If the property is an object, generate a nested class and return its type name.
        /// </summary>
        /// <param name="token"></param>
        /// <param name="propName"></param>
        /// <param name="nestedClasses"></param>
        /// <returns></returns>
        private string DeterminePropertyType(JToken token, string propName)
        {
            propName = propName.ToPascalCase(); // ensure pascal case
            if (token.Type == JTokenType.Array)
            {
                var arr = token as JArray;
                if (arr != null && arr.HasValues)
                {
                    var firstItem = arr[0];
                    if (firstItem.Type == JTokenType.Object)
                    {
                        // Generate nested class for array of objects
                        var nestedTypeName = propName + "Item";
                        GenerateNestedClass(nestedTypeName, (JObject)firstItem);
                        return $"IEnumerable<{nestedTypeName}>";
                    }
                    else
                    {
                        return $"IEnumerable<{GetCSharpTypeForJTokenType(firstItem.Type)}>";
                    }
                }
                else
                {
                    return "IEnumerable<string>";
                }
            }

            if (token.Type == JTokenType.Object)
            {
                var nestedTypeName = propName;
                GenerateNestedClass(nestedTypeName, (JObject)token);
                return nestedTypeName;
            }
            return GetCSharpTypeForJTokenType(token.Type);
        }
        
        /// <summary>
        /// Generate a nested class for an object property and return its code as a string.
        /// </summary>
        private string GenerateNestedClass(string className, JObject obj)
        {
            var separateNode = alwaysSeparateNodeProps.FirstOrDefault(p => p.Name.Equals(className, StringComparison.InvariantCultureIgnoreCase));
            var isSeparateNode = separateNode != null;
            if(separateNode != null && ! string.IsNullOrWhiteSpace(separateNode.ClassName))
                className = separateNode.ClassName;

            var sb = new StringBuilder();
            sb.AppendLine($"    public class {className}{(isSeparateNode ? " : GraphNode" : "")}");
            sb.AppendLine("    {");
            foreach (var prop in obj.Properties())
            {
                sb.AppendLine();
                // Always check if the property should be included
                JArray sampleArray;
                if (obj.Parent is JProperty { Value: JArray arr })
                {
                    sampleArray = arr;
                }
                else if (obj.Parent is JArray arr2)
                {
                    sampleArray = arr2;
                }
                else
                {
                    sampleArray = new JArray(obj);
                }
                var include = ShouldIncludeProperty(prop, sampleArray);
                if (include)
                {

                    // Only add NodeProperty for non-relationship, non-nested class properties
                    if (nestedClassNames.Any(nc => nc == prop.Name.ToPascalCase() || nc == prop.Name.ToPascalCase() + "Item"))
                        sb.AppendLine($"        [NodeProperty(\"\")]");
                    else
                    {
                        // Special case: Properties class uses nameof(Property)
                        var nodePropertyAttr = isSeparateNode ||
                                               dontIncludeClassNameInNodePropertyName.Contains(className)
                            ? $"[NodeProperty(nameof({prop.Name.ToPascalCase()}))]" 
                            : $"[NodeProperty(\"{className}-{prop.Name.ToPascalCase()}\")]";

                        sb.AppendLine($"        {nodePropertyAttr}");
                    }

                }
                sb.AppendLine($"        public {DeterminePropertyType(prop.Value, prop.Name)} {prop.Name.ToPascalCase()} {{ get; set; }}");
            }

            sb.AppendLine();
            if (isSeparateNode)
            {
                var displayProperty = string.IsNullOrWhiteSpace(separateNode!.DisplayName) ? "Name" : separateNode.DisplayName;
                sb.AppendLine($"        public override string BuildDisplayName() => {displayProperty};");
            }

            sb.AppendLine("    }");
            return sb.ToString();
        }


        /// <summary>
        /// Shared method to save generated code to a file
        /// </summary>
        /// <returns></returns>
        private async Task SaveGeneratedFile(string subDirectory, string fileName, string code)
        {
#if DEBUG
            // Find the project root (directory containing a .csproj file)
            string FindProjectRoot(string startDir)
            {
                var dir = new DirectoryInfo(startDir);
                while (dir != null && !dir.GetFiles("*.csproj").Any())
                {
                    dir = dir.Parent;
                }
                return dir?.FullName ?? startDir;
            }
            var root = FindProjectRoot(Directory.GetCurrentDirectory());
#else
            var root = AppContext.BaseDirectory;
#endif
            var outputDirectory = Path.Combine(root, "Generated", subDirectory);
            Directory.CreateDirectory(outputDirectory);
            var filePath = Path.Combine(outputDirectory, fileName);
            await File.WriteAllTextAsync(filePath, code);
            logger.LogInformation("Generated file: {FilePath}", filePath);
        }

        /// <summary>
        /// Determines if a property should be included based on whether
        /// any item in the training data array has a non-null value for it (and non empty string)
        /// Now also respects AlwaysIncludeProperty and AlwaysExcludeProperty from config.
        /// </summary>
        /// <param name="prop">The JProperty to check.</param>
        /// <param name="trainingData">The full JArray of training data.</param>
        /// <returns>True if any item has a non-null value for the property; otherwise, false.</returns>
        private bool ShouldIncludeProperty(JProperty prop, JArray trainingData)
        {
            var propName = prop.Name;
            // Always exclude if in alwaysExcludeProperties
            if (alwaysExcludeProperties.Any(p => p.Equals(propName, StringComparison.InvariantCultureIgnoreCase)))
                return false;
            // Always include if in alwaysIncludeProperties
            if (alwaysIncludeProperties.Any(p => p.Equals(propName, StringComparison.InvariantCultureIgnoreCase)))
                return true;
            foreach (var item in trainingData)
            {
                if (item is JObject obj && obj.TryGetValue(propName, out var value))
                {
                    if (value.Type == JTokenType.Null)
                        continue;
                    if (value.Type == JTokenType.String)
                    {
                        var str = value.ToString();
                        if (!string.IsNullOrEmpty(str))
                            return true;
                    }
                    else if (value.Type == JTokenType.Array)
                    {
                        if (value is JArray { Count: > 0 })
                            return true;
                    }
                    else
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        // Helper class to represent node property info
        private class NodePropertyInfo(string name, string type, bool isPrimaryKey, string relatedType, AlwaysSeparateNodeProperty? separateNode)
        {
            public override string ToString() => $"{Type} {Name}{(IsPrimaryKey ? " (PK)" : "")}";

            public string Name { get; set; } = name;
            public string Type { get; set; } = type;
            public bool IsPrimaryKey { get; set; } = isPrimaryKey;

            public bool IsSeparateNode => separateNode != null;
            public AlwaysSeparateNodeProperty? SeparateNode { get; set; } = separateNode;

            public string RelatedType { get; set; } = relatedType;
        }
    }
}