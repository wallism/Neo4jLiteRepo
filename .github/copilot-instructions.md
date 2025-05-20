# Neo4jLiteRepo Copilot Instructions

## Overview

Neo4jLiteRepo is a library designed to simplify Neo4j database interactions for .NET developers. This library follows specific patterns to make it easier to work with Neo4j graph databases, focusing on creating and querying nodes and relationships.

When assisting with this codebase, please follow these guidelines:

## Project Structure

- **Neo4jLiteRepo** - The core library with all essential functionality
- **Neo4jLiteRepo.Importer** - Sample project that demonstrates how to import data into Neo4j
- **Neo4jLiteRepo.Sample** - Sample models and node services showing how to use the library
- **Neo4jLiteRepo.Tests** - Unit tests for the library

## Key Concepts

### GraphNode Base Class

All node models inherit from `GraphNode`. This base class provides essential functionality:
- Unique identifier property (`Id`)
- Display name generation
- Primary key handling (using `[NodePrimaryKey]` attribute)

### Core Attributes

- **NodePrimaryKeyAttribute**: Marks a property as the node's unique identifier
- **NodePropertyAttribute**: Marks properties that should be stored in Neo4j
- **NodeRelationshipAttribute<T>**: Defines relationships between nodes

### Node Services

Each node type requires a corresponding node service that implements `INodeService`. These services are responsible for:
- Loading data
- Providing type information to the repository
- Configuring unique constraints

### Neo4jGenericRepo

This is the central class for database interactions. It handles:
- Creating and upserting nodes
- Creating relationships between nodes
- Enforcing unique constraints
- Executing Cypher queries

## Code Generation Guidelines

### Creating Node Classes

1. Always inherit from `GraphNode`
2. Mark one property with `[NodePrimaryKey]`
3. Use `[NodeProperty]` for properties to be stored in Neo4j (do NOT add to properties where there is only null values in the TrainFromJson)
4. Use `[NodeRelationshipAttribute<T>]` for relationship properties
5. Implement the abstract `BuildDisplayName()` method

Example:
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

### Creating Node Services

1. Inherit from appropriate base service (usually `FileNodeService<T>`)
2. Implement required methods for loading data
3. Register services in the IoC container

Example:
```csharp
public class MovieNodeService : FileNodeService<Movie>
{
    public MovieNodeService(IConfiguration config, IDataRefreshPolicy dataRefreshPolicy) : base(config, dataRefreshPolicy)
    {
    }
}

// In startup/program:
builder.Services.AddSingleton<INodeService, MovieNodeService>();
```

### Working with Relationships

- Relationships require properties of type `IEnumerable<string>`
- Values should contain primary key values of related nodes
- Both sides of a relationship should be defined (bidirectional)

## Best Practices

1. Use descriptive relationship names in UPPERCASE_WITH_UNDERSCORES format
2. Ensure primary key properties are unique within their node type
3. Register all node services in the DI container
4. Configure connection settings in appsettings.json
5. Use the dataSourceService to access nodes across the application
6. Always include a custom implementation of `BuildDisplayName()`

## .Net C# Recommendations
1. Use `var` for local variable declarations when the type is clear from the right-hand side
2. Use collection expressions where possible (e.g., `var list = [ "item1", "item2" ];`)

## Cypher Query Recommendations

When writing custom Cypher queries:
1. Use parameterized queries to prevent injection
2. Use MERGE for nodes that might already exist
3. Use proper relationship direction (->/<-) based on the domain model
4. Consider using the repository's built-in methods before writing custom queries

## Neo4j Configuration

The application expects Neo4j connection details in appsettings.json:
```json
{
  "Neo4jSettings": {
    "Connection": "neo4j://localhost:7687", 
    "User": "neo4j",
    "Password": "your-password"
  }
}
```

For AuraDB, use the instance ID in the Connection string instead of "localhost".

## String Construction Guidelines

- When creating multi-line strings, use C# raw string literals (triple quotes) and string interpolation where possible.
- Do **not** use `StringBuilder` for code generation or multi-line string construction unless there is a clear performance or mutability requirement.
- Prefer readable, idiomatic C# string construction for templates and generated code.

## Child Object Modeling Guidelines
When deciding whether to model a child object as a separate node:

Model as a separate node if:

- The object is reused across multiple parent nodes (e.g., SKU, Tag).
- The object has a complex structure or nested properties.
- The object is meaningful on its own and may be queried or related independently.
- The object’s data is likely to be analyzed, filtered, or joined across multiple parent nodes.
Otherwise, flatten as properties (default behaviour in the code).

Only model as a separate node if you foresee querying or relating the child object independently, or if the structure is complex and may evolve.
Relationship Naming:

Use descriptive, UPPERCASE_WITH_UNDERSCORES relationship names (e.g., HAS_SKU, HAS_NETWORK_ACLS).

## git
- main branch is 'master'
