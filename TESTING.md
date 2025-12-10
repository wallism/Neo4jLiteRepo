# Neo4jLiteRepo Test Projects

This solution contains two separate test projects to enable flexible test execution:

## Test Projects

### 1. Neo4jLiteRepo.UnitTests
Contains unit tests that use mocks and do not require a live Neo4j database.

**Location:** `src/Neo4jLiteRepo.Tests/`

**Tests include:**
- `Neo4jGenericRepoTests.cs` - Unit tests for repository methods using NSubstitute mocks
- `DataSourceServiceTests.cs` - Unit tests for data source service
- `AutoRedactTests.cs` - Unit tests for auto-redaction functionality
- `ForceRefreshHandlerTests.cs` - Unit tests for force refresh handler

### 2. Neo4jLiteRepo.IntegrationTests
Contains integration tests that require a live Neo4j database instance.

**Location:** `src/Neo4jLiteRepo.IntegrationTests/`

**Tests include:**
- `CrudIntegrationTests.cs` - CRUD operation tests
- `MaintenanceIntegrationTests.cs` - Maintenance operation tests
- `ReadOperationsIntegrationTests.cs` - Read operation tests
- `RelationshipIntegrationTests.cs` - Relationship management tests
- `SchemaIntegrationTests.cs` - Schema operation tests
- `SeedDataIntegrationTests.cs` - Data seeding tests
- `Neo4jIntegrationTestBase.cs` - Base class for integration tests

## Running Tests

### Run All Tests
```powershell
dotnet test src/Neo4jLiteRepo.sln
```

### Run Unit Tests Only
```powershell
dotnet test src/Neo4jLiteRepo.Tests/Neo4jLiteRepo.Tests.csproj
```

### Run Integration Tests Only
```powershell
dotnet test src/Neo4jLiteRepo.IntegrationTests/Neo4jLiteRepo.IntegrationTests.csproj
```

### Run Specific Test Class
```powershell
# Unit test example
dotnet test src/Neo4jLiteRepo.Tests/Neo4jLiteRepo.Tests.csproj --filter "FullyQualifiedName~Neo4jGenericRepoTests"

# Integration test example
dotnet test src/Neo4jLiteRepo.IntegrationTests/Neo4jLiteRepo.IntegrationTests.csproj --filter "FullyQualifiedName~CrudIntegrationTests"
```

### Run with Verbosity
```powershell
dotnet test src/Neo4jLiteRepo.sln --verbosity detailed
```

## Prerequisites

### Unit Tests
- No external dependencies required
- Can run in any environment

### Integration Tests
- **Requires a running Neo4j instance**
- Configure connection in `appsettings.json`:
  ```json
  {
    "Neo4jSettings": {
      "Connection": "neo4j://localhost:7687",
      "User": "neo4j",
      "Password": "your-password"
    }
  }
  ```
- Can use Docker to run Neo4j:
  ```powershell
  docker run -d --name neo4j-test `
    -p 7474:7474 -p 7687:7687 `
    -e NEO4J_AUTH=neo4j/testpassword `
    neo4j:latest
  ```

## Test Framework

Both projects use:
- **NUnit 4.x** - Test framework
- **NSubstitute** - Mocking library for unit tests
- **Serilog** - Logging
- **.NET 10.0** - Target framework

## Best Practices

1. **Run unit tests frequently** during development for fast feedback
2. **Run integration tests** before committing to ensure database operations work correctly
3. **Use integration tests** to validate complex scenarios and real database behavior
4. **Keep unit tests fast** by using mocks instead of real dependencies
5. **Ensure integration tests are idempotent** and clean up after themselves
