using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.NodeServices;
using Neo4jLiteRepo.Sample.Nodes;
using Neo4jLiteRepo.Sample.NodeServices;
using NUnit.Framework;
using Serilog;

namespace Neo4jLiteRepo.Tests.Integration;

/// <summary>
/// Integration tests for Neo4jGenericRepo CRUD operations.
/// 
/// IMPORTANT - WIPES the database during cleanup!
/// Do NOT point at an important database! (hardcoded connection is intentional to prevent accidentally wiping a production DB)
/// 
/// CONFIGURATION:
/// 1. Update Neo4jPassword below to match your Neo4j Desktop database password
/// 2. The tests will create and use a separate 'IntegrationTests' database (requires Enterprise/AuraDB)
/// 3. If using Community Edition, change TestDatabase to "neo4j" (Community only supports one database)
/// </summary>
[TestFixture]
public class CrudIntegrationTests
{
    private IHost _host = null!;
    private IDriver _driver = null!;
    private INeo4jGenericRepo _repo = null!;
    private const string TestDatabase = "IntegrationTests";
    
    // TODO: Update Neo4jPassword to match your Neo4j Desktop instance password
    // All databases in the same Neo4j instance share the same credentials
    private const string Neo4jUri = "neo4j://127.0.0.1:7687"; // Matches your Neo4j Desktop
    private const string Neo4jUser = "neo4j";

    [OneTimeSetUp]
    public async Task OneTimeSetup()
    {
        Log.Logger = new LoggerConfiguration()
            .WriteTo.Console()
            .WriteTo.Debug()
            .CreateLogger();

        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddSerilog(Log.Logger);

        builder.Configuration
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: true)
            .AddUserSecrets(typeof(CrudIntegrationTests).Assembly)
            .AddEnvironmentVariables();

        // Register Neo4j driver - deliberately using hardcoded values because this test wipes the DB during cleanup
        builder.Services.AddSingleton<IDriver>(_ =>
        {
            var password = builder.Configuration["Neo4jSettings:Password"];
            var driver = GraphDatabase.Driver(Neo4jUri, AuthTokens.Basic(Neo4jUser, password));

            Log.Information("neo4j driver initialized for CRUD tests");
            Task.Run(() => Task.FromResult(driver.VerifyConnectivityAsync())).GetAwaiter().GetResult();
            Log.Information("neo4j connectivity verified");
            return driver;
        });

        // Configure the database name for the repository to use
        builder.Configuration["Neo4jSettings:Database"] = TestDatabase;

        builder.Services.AddSingleton<IDataRefreshPolicy, DataRefreshPolicy>();

        // Register node services
        var nodeServiceType = typeof(INodeService);
        var assembliesToScan = new[] { typeof(MovieNodeService).Assembly };
        var nodeServiceImplementations = assembliesToScan
            .SelectMany(assembly => assembly.GetTypes())
            .Where(type => nodeServiceType.IsAssignableFrom(type) && !type.IsInterface && !type.IsAbstract);

        foreach (var implementation in nodeServiceImplementations)
        {
            builder.Services.AddSingleton(nodeServiceType, implementation);
        }

        builder.Services.AddSingleton<INeo4jGenericRepo, Neo4jGenericRepo>();
        builder.Services.AddSingleton<IDataSourceService, DataSourceService>();

        _host = builder.Build();
        _driver = _host.Services.GetRequiredService<IDriver>();
        _repo = _host.Services.GetRequiredService<INeo4jGenericRepo>();

        // Ensure test database exists
        await EnsureDatabaseExists();

        // Clean up before tests to start with empty state
        await CleanupDatabase();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await CleanupDatabase();
        await _driver.DisposeAsync();
        _repo.Dispose();
        _host.Dispose();
    }

    [SetUp]
    public async Task SetUp()
    {
        // Clean up before each test to ensure clean state
        await CleanupDatabase();
    }

    [TearDown]
    public async Task TearDown()
    {
        // Clean up after each test as well for good measure
        await CleanupDatabase();
    }

    #region UpsertNode Tests

    [Test]
    public async Task UpsertNode_CreatesNewNode()
    {
        // Arrange
        var movie = new Movie
        {
            Id = "test-1",
            Title = "Test Movie",
            Released = 2023,
            Tagline = "A test movie"
        };

        // Act
        await _repo.UpsertNode(movie);

        // Assert
        var loaded = await _repo.LoadAsync<Movie>("test-1");
        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.Title, Is.EqualTo("Test Movie"));
        Assert.That(loaded.Released, Is.EqualTo(2023));
    }

    [Test]
    public async Task UpsertNode_UpdatesExistingNode()
    {
        // Arrange
        var movie = new Movie
        {
            Id = "test-2",
            Title = "Original Title",
            Released = 2020,
            Tagline = "Original tagline"
        };
        await _repo.UpsertNode(movie);

        // Act - Update the same node
        movie.Title = "Updated Title";
        movie.Released = 2021;
        await _repo.UpsertNode(movie);

        // Assert
        var loaded = await _repo.LoadAsync<Movie>("test-2");
        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.Title, Is.EqualTo("Updated Title"));
        Assert.That(loaded.Released, Is.EqualTo(2021));
    }

    [Test]
    public async Task UpsertNodes_CreatesMultipleNodes()
    {
        // Arrange
        var movies = new List<Movie>
        {
            new() { Id = "batch-1", Title = "Movie 1", Released = 2021, Tagline = "Tag 1" },
            new() { Id = "batch-2", Title = "Movie 2", Released = 2022, Tagline = "Tag 2" },
            new() { Id = "batch-3", Title = "Movie 3", Released = 2023, Tagline = "Tag 3" }
        };

        // Act
        await _repo.UpsertNodes(movies);

        // Assert - Verify all three nodes exist
        var allMovies = await _repo.LoadAllAsync<Movie>();
        Assert.That(allMovies.Any(m => m.Id == "batch-1"), Is.True);
        Assert.That(allMovies.Any(m => m.Id == "batch-2"), Is.True);
        Assert.That(allMovies.Any(m => m.Id == "batch-3"), Is.True);
    }

    #endregion

    #region LoadAsync Tests

    [Test]
    public async Task LoadAsync_ReturnsNullForNonExistentNode()
    {
        // Act
        var loaded = await _repo.LoadAsync<Movie>("non-existent-id");

        // Assert
        Assert.That(loaded, Is.Null);
    }

    [Test]
    public async Task LoadAllAsync_ReturnsAllNodes()
    {
        // Arrange
        await _repo.UpsertNode(new Movie { Id = "load-1", Title = "Movie 1", Released = 2021, Tagline = "Tag 1" });
        await _repo.UpsertNode(new Movie { Id = "load-2", Title = "Movie 2", Released = 2022, Tagline = "Tag 2" });

        // Act
        var movies = await _repo.LoadAllAsync<Movie>();

        // Assert - Verify both test nodes exist (may have other nodes from other tests)
        Assert.That(movies.Any(m => m.Id == "load-1"), Is.True);
        Assert.That(movies.Any(m => m.Id == "load-2"), Is.True);
    }

    [Test]
    public async Task LoadAllAsync_WithPagination_ReturnsCorrectPage()
    {
        // Arrange
        for (int i = 1; i <= 5; i++)
        {
            await _repo.UpsertNode(new Movie { Id = $"page-{i}", Title = $"Movie {i}", Released = 2020 + i, Tagline = $"Tag {i}" });
        }

        // Act - Get page 2 (skip 2, take 2)
        var page = await _repo.LoadAllAsync<Movie>(skip: 2, take: 2);

        // Assert
        Assert.That(page.Count, Is.EqualTo(2));
    }

    #endregion

    #region DetachDeleteAsync Tests

    [Test]
    public async Task DetachDeleteAsync_DeletesNode()
    {
        // Arrange
        var movie = new Movie { Id = "delete-1", Title = "To Delete", Released = 2023, Tagline = "Will be deleted" };
        await _repo.UpsertNode(movie);

        // Verify it exists
        var beforeDelete = await _repo.LoadAsync<Movie>("delete-1");
        Assert.That(beforeDelete, Is.Not.Null);

        // Act
        await _repo.DetachDeleteAsync(movie);

        // Assert
        var afterDelete = await _repo.LoadAsync<Movie>("delete-1");
        Assert.That(afterDelete, Is.Null);
    }

    [Test]
    public async Task DetachDeleteAsync_ByPrimaryKey_DeletesNode()
    {
        // Arrange
        await _repo.UpsertNode(new Movie { Id = "delete-pk", Title = "To Delete", Released = 2023, Tagline = "Delete by PK" });

        // Verify it exists
        var beforeDelete = await _repo.LoadAsync<Movie>("delete-pk");
        Assert.That(beforeDelete, Is.Not.Null);

        // Act
        await _repo.DetachDeleteAsync<Movie>("delete-pk");

        // Assert
        var afterDelete = await _repo.LoadAsync<Movie>("delete-pk");
        Assert.That(afterDelete, Is.Null);
    }

    [Test]
    public async Task DetachDeleteManyAsync_DeletesMultipleNodes()
    {
        // Arrange
        await _repo.UpsertNodes(new List<Movie>
        {
            new() { Id = "del-many-1", Title = "Delete 1", Released = 2021, Tagline = "Tag 1" },
            new() { Id = "del-many-2", Title = "Delete 2", Released = 2022, Tagline = "Tag 2" },
            new() { Id = "del-many-3", Title = "Keep", Released = 2023, Tagline = "Tag 3" }
        });

        // Act - Delete first two
        await _repo.DetachDeleteManyAsync<Movie>(new List<string> { "del-many-1", "del-many-2" });

        // Assert - Verify deleted nodes are gone and kept node remains
        var deleted1 = await _repo.LoadAsync<Movie>("del-many-1");
        var deleted2 = await _repo.LoadAsync<Movie>("del-many-2");
        var kept = await _repo.LoadAsync<Movie>("del-many-3");
        Assert.That(deleted1, Is.Null);
        Assert.That(deleted2, Is.Null);
        Assert.That(kept, Is.Not.Null);
    }

    #endregion

    #region MergeRelationshipAsync Tests

    [Test]
    public async Task MergeRelationshipAsync_CreatesRelationship()
    {
        // Arrange
        var movie = new Movie { Id = "rel-movie", Title = "Related Movie", Released = 2023, Tagline = "Has genre" };
        var genre = new Genre { Id = "rel-genre", Name = "Action" };
        await _repo.UpsertNode(movie);
        await _repo.UpsertNode(genre);

        // Act
        await _repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);

        // Assert - Load movie and check relationship
        var loadedMovie = await _repo.LoadAsync<Movie>("rel-movie");
        Assert.That(loadedMovie, Is.Not.Null);
        Assert.That(loadedMovie!.GenreIds, Does.Contain("rel-genre"));
    }

    [Test]
    public async Task DeleteRelationshipAsync_RemovesRelationship()
    {
        // Arrange
        var movie = new Movie { Id = "del-rel-movie", Title = "Movie", Released = 2023, Tagline = "Tag" };
        var genre = new Genre { Id = "del-rel-genre", Name = "Comedy" };
        await _repo.UpsertNode(movie);
        await _repo.UpsertNode(genre);
        await _repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);

        // Verify relationship exists
        var beforeDelete = await _repo.LoadAsync<Movie>("del-rel-movie");
        Assert.That(beforeDelete, Is.Not.Null);
        Assert.That(beforeDelete!.GenreIds, Does.Contain("del-rel-genre"));
        var genreCountBefore = beforeDelete.GenreIds?.Count() ?? 0;

        // Act
        await _repo.DeleteRelationshipAsync(movie, "IN_GENRE", genre, Neo4jLiteRepo.Models.EdgeDirection.Outgoing);

        // Assert
        var afterDelete = await _repo.LoadAsync<Movie>("del-rel-movie");
        Assert.That(afterDelete, Is.Not.Null);
        Assert.That(afterDelete!.GenreIds, Does.Not.Contain("del-rel-genre"));
        var genreCountAfter = afterDelete.GenreIds?.Count() ?? 0;
        Assert.That(genreCountAfter, Is.EqualTo(genreCountBefore - 1));
    }

    #endregion

    #region ExecuteReadListAsync Tests

    [Test]
    public async Task ExecuteReadListAsync_ReturnsMatchingNodes()
    {
        // Arrange
        await _repo.UpsertNodes(new List<Movie>
        {
            new() { Id = "read-1", Title = "Action Movie", Released = 2021, Tagline = "Tag" },
            new() { Id = "read-2", Title = "Comedy Movie", Released = 2022, Tagline = "Tag" }
        });

        // Act - Filter for movies from this test only
        var results = await _repo.ExecuteReadListAsync<Movie>(
            "MATCH (m:Movie) WHERE m.id IN $ids AND m.released >= $year RETURN m AS movie",
            "movie",
            new Dictionary<string, object> 
            { 
                { "ids", new[] { "read-1", "read-2" } },
                { "year", 2022 } 
            });

        // Assert
        var resultList = results.ToList();
        Assert.That(resultList.Count, Is.EqualTo(1));
        Assert.That(resultList[0].Title, Is.EqualTo("Comedy Movie"));
        Assert.That(resultList[0].Id, Is.EqualTo("read-2"));
    }

    #endregion

    #region ExecuteReadScalarAsync Tests

    [Test]
    public async Task ExecuteReadScalarAsync_ReturnsCount()
    {
        // Arrange
        await _repo.UpsertNodes(new List<Movie>
        {
            new() { Id = "count-1", Title = "Movie 1", Released = 2021, Tagline = "Tag" },
            new() { Id = "count-2", Title = "Movie 2", Released = 2022, Tagline = "Tag" },
            new() { Id = "count-3", Title = "Movie 3", Released = 2023, Tagline = "Tag" }
        });

        // Act - Query specifically for the test data
        var count = await _repo.ExecuteReadScalarAsync<long>(
            "MATCH (m:Movie) WHERE m.id IN ['count-1', 'count-2', 'count-3'] RETURN count(m)");

        // Assert
        Assert.That(count, Is.EqualTo(3));
    }

    #endregion

    #region RemoveOrphansAsync Tests

    [Test]
    public async Task RemoveOrphansAsync_RemovesNodesWithNoRelationships()
    {
        // Arrange - Create nodes with and without relationships
        var connectedMovie = new Movie { Id = "connected", Title = "Connected", Released = 2023, Tagline = "Has relation" };
        var orphanMovie = new Movie { Id = "orphan", Title = "Orphan", Released = 2023, Tagline = "No relation" };
        var genre = new Genre { Id = "orphan-test-genre", Name = "Test Genre" };

        await _repo.UpsertNode(connectedMovie);
        await _repo.UpsertNode(orphanMovie);
        await _repo.UpsertNode(genre);
        await _repo.MergeRelationshipAsync(connectedMovie, "IN_GENRE", genre);

        // Verify both movies exist before cleanup
        var connectedBefore = await _repo.LoadAsync<Movie>("connected");
        var orphanBefore = await _repo.LoadAsync<Movie>("orphan");
        Assert.That(connectedBefore, Is.Not.Null);
        Assert.That(orphanBefore, Is.Not.Null);

        // Act - Remove orphan movies
        var removedCount = await _repo.RemoveOrphansAsync<Movie>();

        // Assert - Orphan should be removed, connected should remain
        Assert.That(removedCount, Is.GreaterThanOrEqualTo(1));
        var connectedAfter = await _repo.LoadAsync<Movie>("connected");
        var orphanAfter = await _repo.LoadAsync<Movie>("orphan");
        Assert.That(connectedAfter, Is.Not.Null);
        Assert.That(orphanAfter, Is.Null);
    }

    #endregion

    #region Cleanup Verification Tests

    [Test]
    public async Task Cleanup_EnsuresDatabaseIsEmpty()
    {
        // Arrange - Add some test data
        await _repo.UpsertNode(new Movie { Id = "cleanup-test", Title = "Test", Released = 2023, Tagline = "Tag" });
        var beforeCleanup = await _repo.LoadAllAsync<Movie>();
        Assert.That(beforeCleanup.Any(m => m.Id == "cleanup-test"), Is.True, "Test data should exist before cleanup");

        // Act - Cleanup using direct driver query to verify cleanup method works
        var cleanupSession = _driver.AsyncSession(o => o.WithDatabase(TestDatabase));
        try
        {
            await cleanupSession.ExecuteWriteAsync(async tx =>
            {
                var result = await tx.RunAsync("MATCH (n) DETACH DELETE n");
                await result.ConsumeAsync();
            });
        }
        finally
        {
            await cleanupSession.CloseAsync();
        }

        // Assert - Database should be completely empty (using direct query)
        var verifySession = _driver.AsyncSession(o => o.WithDatabase(TestDatabase));
        try
        {
            var totalCount = await verifySession.ExecuteReadAsync(async tx =>
            {
                var result = await tx.RunAsync("MATCH (n) RETURN count(n) as count");
                var record = await result.SingleAsync();
                return record["count"].As<long>();
            });
            
            Assert.That(totalCount, Is.EqualTo(0), $"Should have no nodes at all after cleanup, but found {totalCount}");
        }
        finally
        {
            await verifySession.CloseAsync();
        }
    }

    #endregion

    #region Helper Methods

    private async Task CleanupDatabase()
    {
        var session = _driver.AsyncSession(o => o.WithDatabase(TestDatabase));
        try
        {
            await session.ExecuteWriteAsync(async tx =>
            {
                var result = await tx.RunAsync("MATCH (n) DETACH DELETE n");
                await result.ConsumeAsync();
            });
        }
        finally
        {
            await session.CloseAsync();
        }
    }

    private async Task EnsureDatabaseExists()
    {
        // Note: Multiple databases require Neo4j Enterprise or AuraDB
        // Community Edition users should set TestDatabase = "neo4j" (the default database)
        
        var session = _driver.AsyncSession(o => o.WithDatabase("system"));
        try
        {
            // Try to create database - IF NOT EXISTS means this is safe to run even if it already exists
            var result = await session.RunAsync($"CREATE DATABASE `{TestDatabase}` IF NOT EXISTS");
            await result.ConsumeAsync();
            Log.Information($"Ensured test database '{TestDatabase}' exists");
            
            // Wait a moment for database to be ready
            await Task.Delay(1000);
        }
        catch (ClientException ex) when (ex.Code.Contains("Unsupported"))
        {
            // Community Edition doesn't support multiple databases
            Log.Warning($"Multiple databases not supported (Community Edition). Please set TestDatabase = \"neo4j\" or upgrade to Enterprise/AuraDB.");
            Log.Warning($"Tests will attempt to use '{TestDatabase}' database anyway.");
        }
        catch (Exception ex)
        {
            Log.Error(ex, $"Failed to create database '{TestDatabase}'. Check your credentials match your Neo4j Desktop database.");
            Log.Information($"Current credentials: User='{Neo4jUser}', Uri='{Neo4jUri}'");
            throw;
        }
        finally
        {
            await session.CloseAsync();
        }
    }

    #endregion
}
