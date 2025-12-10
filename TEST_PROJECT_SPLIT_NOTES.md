# Test Project Split - Manual Steps Required

## ✅ Completed
1. Created new `Neo4jLiteRepo.IntegrationTests` project
2. Moved all integration test files to the new project
3. Updated solution file to include both test projects
4. Updated project references and configuration
5. Verified unit tests run independently (45 tests passed)
6. Created `TESTING.md` documentation

## 📝 Manual Step Required

**Rename the folder:** `Neo4jLiteRepo.Tests` → `Neo4jLiteRepo.UnitTests`

The folder couldn't be renamed automatically because files are currently open/locked in VS Code.

### Steps to complete:
1. Close VS Code/Solution
2. Rename the folder:
   ```powershell
   Rename-Item -Path "c:\Projects\_Mark\Neo4jLiteRepo\src\Neo4jLiteRepo.Tests" -NewName "Neo4jLiteRepo.UnitTests"
   ```
3. Update the solution file path reference:
   ```
   Change: Neo4jLiteRepo.Tests\Neo4jLiteRepo.Tests.csproj
   To:     Neo4jLiteRepo.UnitTests\Neo4jLiteRepo.Tests.csproj
   ```
4. Reopen the solution

**Note:** The project already has the correct assembly name and namespace (`Neo4jLiteRepo.UnitTests`), only the folder name needs updating.

## Project Structure

```
src/
├── Neo4jLiteRepo.Tests/                      (rename to Neo4jLiteRepo.UnitTests)
│   ├── Neo4jGenericRepoTests.cs
│   ├── DataSourceServiceTests.cs
│   ├── AutoRedactTests.cs
│   └── ForceRefreshHandlerTests.cs
│
└── Neo4jLiteRepo.IntegrationTests/
    ├── CrudIntegrationTests.cs
    ├── MaintenanceIntegrationTests.cs
    ├── ReadOperationsIntegrationTests.cs
    ├── RelationshipIntegrationTests.cs
    ├── SchemaIntegrationTests.cs
    ├── SeedDataIntegrationTests.cs
    └── Neo4jIntegrationTestBase.cs
```
