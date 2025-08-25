# Neo4jLiteRepo

Neo4jLiteRepo is a .NET library designed to simplify Neo4j database interactions by providing a clear, attribute-driven pattern for modeling, seeding, and querying graph data. It enables rapid onboarding for .NET developers and supports both local and AuraDB Neo4j instances.

## Project Structure
- **Neo4jLiteRepo**: Core library with all essential functionality
- **Neo4jLiteRepo.Importer**: Sample project demonstrating data import and configuration
- **Neo4jLiteRepo.Sample**: Example node models and node services
- **Neo4jLiteRepo.Tests**: Unit tests for the library


## Key Concepts & Recent Changes

### Node Modeling (`GraphNode`)
- All node models inherit from `GraphNode`.
- Mark one property with `[NodePrimaryKey]` (must be unique).
- Use `[NodeProperty]` for properties to be stored in Neo4j.
- Use `[NodeRelationship<T>]` for relationship properties (must be `IEnumerable<string>` containing primary key values of related nodes).
- Implement the abstract `BuildDisplayName()` method for each node.
- **New:**
  - Improved primary key handling: `GraphNode` now enforces that only one property is decorated with `[NodePrimaryKey]` and throws clear exceptions if not found or misused.
  - Added `GetPrimaryKeyName()` and `GetPrimaryKeyValue()` for robust access to primary key info.
  - Added `GetMainContent()` as an abstract method for custom node content extraction.
  - `LabelName` and `NodeDisplayNameProperty` are now virtual and use helper extensions for casing.
  - `Upserted` property tracks last upsert time (auto-set).

**Example:**
```csharp
public class Movie : GraphNode
{
    [NodePrimaryKey]
    public string Title { get; set; }

    [NodeProperty(nameof(Released))]
    public int Released { get; set; }

    [NodeRelationship<Genre>("IN_GENRE")]
    public IEnumerable<string> Genres { get; set; }

    public override string BuildDisplayName() => Title;
    public override string GetMainContent() => $"{Title} ({Released})";
}
```

### Node Services
- Each node type requires a corresponding node service implementing `INodeService` (usually inherit from `FileNodeService<T>`).
- Responsible for loading data, providing type info, and configuring unique constraints.
- Register all node services in your DI container:
```csharp
builder.Services.AddSingleton<INodeService, MovieNodeService>();
```

### Relationships
- Define relationships using `[NodeRelationship<T>]` as `IEnumerable<string>` properties.
- Relationship names should be descriptive and UPPERCASE_WITH_UNDERSCORES (e.g., `HAS_GENRE`).
- Both sides of a relationship should be defined for bidirectionality if needed.


### Repository Usage (`Neo4jGenericRepo`)
- `Neo4jGenericRepo` is the main entry point for database operations (node/relationship upsert, Cypher queries, constraints).
- **Recent API additions:**
  - Expanded overloads for upserting nodes and relationships, supporting session and transaction management for batching and atomicity.
  - New methods for enforcing unique constraints, creating vector indexes, and streaming query results.
  - Improved error handling and diagnostics for node/relationship operations.
  - Methods for orphan node removal, batch relationship deletion, and vector similarity search.
- **Performance Note:**
  - The repository now passes the parameters dictionary directly to Cypher queries, resulting in significant performance improvements for large and complex operations. Always use parameterized queries for best results.
- Prefer repository methods over custom Cypher unless advanced queries are needed.

### Edge Modeling (`Edge` and `IEdge`)
- The `Edge` class and `IEdge` interface represent relationships between nodes in the graph.
- **Key Features:**
  - `TargetPrimaryKey`: Represents the primary key of the target node in the relationship.
- Inherit from `Edge` when your edge requires custom properties.

**Example:**
```csharp
public class ActedIn : Edge
{
    public string Role { get; set; }
}
```

## Getting Started

### 1. Set Up Neo4j
- **AuraDB**: [Get a free instance](https://neo4j.com/product/auradb/). Use the instance ID for `Neo4jSettings:Connection` in `appsettings.json`.
- **Local Docker**:
```powershell

$now = Get-Date -Format "yyyyMMdd"
$product = "neo4jlite"
docker run -d --rm `
  --name neo4j-$product-$now `
  -e server.memory.heap.initial_size=1G `
  -e server.memory.heap.max_size=4G `
  -e server.memory.pagecache.size=2G `
  -v C:\Projects\yourproject\volumedata-${product}:/data `
  -p 7474:7474 `
  -p 7687:7687 `
  --memory="7g" `
  neo4j:latest
```
**make sure you have in your .gitignore:**

volumedata/

### 2. Configure Your Project
- Copy the `Neo4jLiteRepo` project into your solution (NuGet not created yet).
- Copy `.Importer` and optionally `.Sample` for reference.
- Create your own project for node models and services, following the `.Sample` structure.
- Configure Neo4j connection in `appsettings.json`:
```json
{
  "Neo4jSettings": {
    "Connection": "neo4j://localhost:7687", 
    "User": "neo4j",
    "Password": "your-password"
  }
}
```

### 3. Create Nodes and Relationships
- Add node classes (inheriting from `GraphNode`) and decorate with attributes as described above.
- Add node services for each node type.
- Register all node services in your DI container.

### 4. Import Data
- Use the Importer project or your own logic to seed data.
- Relationships are created by matching primary key values in related node lists.

### 5. Querying
- Use the repository's built-in methods for most queries.
- For custom Cypher, use parameterized queries and prefer `MERGE` for upserts.
- Example to view all nodes and relationships:
```cypher
MATCH (n)
OPTIONAL MATCH (n)-[r]-(m)
RETURN n, r, m
```

## NodeTrainer Usage

`NodeTrainer` is a utility for generating node classes and node service classes from sample JSON data. This helps automate the creation of models and services that follow Neo4jLiteRepo conventions.

### How It Works
- Reads JSON files from a configured directory (see `Neo4jLiteRepo:TrainingInputDirectory` in your `appsettings.json`).
- Analyzes the structure of each JSON file to generate C# node classes (inheriting from `GraphNode`) and corresponding node service classes.
- Generated files are saved under `Node.Training/Generated/Nodes` and `Node.Training/Generated/NodeServices`.

### Configuration
- Configure the input directory and property handling in `Node.Training/appsettings.json`:
  - `Neo4jLiteRepo:TrainingInputDirectory`: Directory containing your sample JSON files.
  - `AlwaysSeparateNodeProps`, `AlwaysIncludeProperty`, `AlwaysExcludeProperty`, etc., control how properties are modeled.

### Running NodeTrainer
1. Place your sample JSON files in the configured training directory.
2. Run the NodeTrainer app (from the `Node.Training` project):
   ```powershell
   dotnet run --project src/Node.Training/Node.Training.csproj
   ```
3. Generated C# files will appear in the `Node.Training/Generated` folder.
4. Review and move generated files into your main node model and service projects as needed.

> **Tip:** Adjust configuration to control which properties are modeled as relationships, which are included/excluded, and naming conventions.


## Best Practices
- Use descriptive, unique primary keys for each node type.
- Use UPPERCASE_WITH_UNDERSCORES for relationship names.
- Only model child objects as separate nodes if they are reused, complex, or independently queried.
- Always implement `BuildDisplayName()` and `GetMainContent()` for each node.
- Use C# raw string literals for multi-line strings.
- Prefer idiomatic, readable C# over premature optimization.
- Use repository overloads for efficient batching and atomic operations.

## Logging & Configuration
- The `.Importer` project uses [Serilog](https://github.com/serilog/serilog`) for logging (optional).
- All Neo4j connection/configuration is handled via `appsettings.json`.

## Contributing
Contributions are welcome! Please see our [CONTRIBUTING.md](CONTRIBUTING.md) for more details.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact
If you have any questions, feel free to reach out.

Happy coding!