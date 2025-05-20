# Neo4jLiteRepo

Neo4jLiteRepo is a .NET library designed to simplify Neo4j database interactions by providing a clear, attribute-driven pattern for modeling, seeding, and querying graph data. It enables rapid onboarding for .NET developers and supports both local and AuraDB Neo4j instances.

## Project Structure
- **Neo4jLiteRepo**: Core library with all essential functionality
- **Neo4jLiteRepo.Importer**: Sample project demonstrating data import and configuration
- **Neo4jLiteRepo.Sample**: Example node models and node services
- **Neo4jLiteRepo.Tests**: Unit tests for the library

## Key Concepts

### Node Modeling
- All node models inherit from `GraphNode`.
- Mark one property with `[NodePrimaryKey]` (must be unique).
- Use `[NodeProperty]` for properties to be stored in Neo4j.
- Use `[NodeRelationship<T>]` for relationship properties (must be `IEnumerable<string>` containing primary key values of related nodes).
- Implement the abstract `BuildDisplayName()` method for each node.

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

### Repository Usage
- `Neo4jGenericRepo` is the main entry point for database operations (node/relationship upsert, Cypher queries, constraints).
- Prefer repository methods over custom Cypher unless advanced queries are needed.

## Getting Started

### 1. Set Up Neo4j
- **AuraDB**: [Get a free instance](https://neo4j.com/product/auradb/). Use the instance ID for `Neo4jSettings:Connection` in `appsettings.json`.
- **Local Docker**:
```powershell
$neo4jpassword = "YourPassword"
$today = Get-Date -Format "yyyyMMdd"
docker run -d --rm `
  --name neo4j-$today `
  -e NEO4J_AUTH=neo4j/$neo4jpassword `
  -e NEO4J_dbms_memory_heap_initial__size=512m `
  -e NEO4J_dbms_memory_heap_max__size=1G `
  -v C:/Projects/YourNeo4jProject/data:/data `
  -p 7474:7474 `
  -p 7687:7687 `
  neo4j:latest
```

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

## Best Practices
- Use descriptive, unique primary keys for each node type.
- Use UPPERCASE_WITH_UNDERSCORES for relationship names.
- Only model child objects as separate nodes if they are reused, complex, or independently queried.
- Always implement `BuildDisplayName()` for each node.
- Use C# raw string literals for multi-line strings.
- Prefer idiomatic, readable C# over premature optimization.

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