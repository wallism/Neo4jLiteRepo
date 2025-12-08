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
/// IMPORTANT - WIPES the database during cleanup,
/// so do NOT point at an important database! (hardcoded connection is intentional to help prevent accidentally wiping a DB)
/// </summary>
[TestFixture]
public class CrudIntegrationTests
{
    private IHost _host = null!;
    private IDriver _driver = null!;
    private INeo4jGenericRepo _repo = null!;
    private const string TestDatabase = "IntegrationTests";

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
            var driver = GraphDatabase.Driver("neo4j://localhost:7687",
                AuthTokens.Basic("neo4j", "password-for-unit-testing-db-only"));

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

    [TearDown]
    public async Task TearDown()
    {
        // Clean up after each test to ensure isolation
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

        // Assert
        var allMovies = await _repo.LoadAllAsync<Movie>();
        Assert.That(allMovies.Count, Is.EqualTo(3));
        Assert.That(allMovies.Any(m => m.Id == "batch-1"));
        Assert.That(allMovies.Any(m => m.Id == "batch-2"));
        Assert.That(allMovies.Any(m => m.Id == "batch-3"));
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

        // Assert
        Assert.That(movies.Count, Is.EqualTo(2));
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

        // Assert
        var remaining = await _repo.LoadAllAsync<Movie>();
        Assert.That(remaining.Count, Is.EqualTo(1));
        Assert.That(remaining[0].Id, Is.EqualTo("del-many-3"));
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
        Assert.That(beforeDelete!.GenreIds, Does.Contain("del-rel-genre"));

        // Act
        await _repo.DeleteRelationshipAsync(movie, "IN_GENRE", genre, Neo4jLiteRepo.Models.EdgeDirection.Outgoing);

        // Assert
        var afterDelete = await _repo.LoadAsync<Movie>("del-rel-movie");
        Assert.That(afterDelete!.GenreIds, Does.Not.Contain("del-rel-genre"));
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

        // Act
        var results = await _repo.ExecuteReadListAsync<Movie>(
            "MATCH (m:Movie) WHERE m.released >= $year RETURN m AS movie",
            "movie",
            new Dictionary<string, object> { { "year", 2022 } });

        // Assert
        var resultList = results.ToList();
        Assert.That(resultList.Count, Is.EqualTo(1));
        Assert.That(resultList[0].Title, Is.EqualTo("Comedy Movie"));
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

        // Act
        var count = await _repo.ExecuteReadScalarAsync<long>("MATCH (m:Movie) RETURN count(m)");

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

        // Verify both movies exist
        var allMoviesBefore = await _repo.LoadAllAsync<Movie>();
        Assert.That(allMoviesBefore.Count, Is.EqualTo(2));

        // Act - Remove orphan movies
        var removedCount = await _repo.RemoveOrphansAsync<Movie>();

        // Assert
        Assert.That(removedCount, Is.EqualTo(1));
        var allMoviesAfter = await _repo.LoadAllAsync<Movie>();
        Assert.That(allMoviesAfter.Count, Is.EqualTo(1));
        Assert.That(allMoviesAfter[0].Id, Is.EqualTo("connected"));
    }

    #endregion

    #region Helper Methods

    private async Task CleanupDatabase()
    {
        var session = _driver.AsyncSession(o => o.WithDatabase(TestDatabase));
        try
        {
            await session.RunAsync("MATCH (n) DETACH DELETE n");
        }
        finally
        {
            await session.CloseAsync();
        }
    }

    private async Task EnsureDatabaseExists()
    {
        var session = _driver.AsyncSession(o => o.WithDatabase("system"));
        try
        {
            // Check if database exists
            var result = await session.RunAsync(
                "SHOW DATABASES YIELD name WHERE name = $dbName RETURN name",
                new { dbName = TestDatabase });
            
            var databases = await result.ToListAsync();
            
            if (databases.Count == 0)
            {
                // Create database if it doesn't exist
                await session.RunAsync($"CREATE DATABASE {TestDatabase} IF NOT EXISTS");
                Log.Information($"Created test database: {TestDatabase}");
                
                // Wait a moment for database to be ready
                await Task.Delay(2000);
            }
        }
        catch (Exception ex)
        {
            Log.Warning($"Could not verify/create database {TestDatabase}: {ex.Message}");
        }
        finally
        {
            await session.CloseAsync();
        }
    }

    #endregion
}
