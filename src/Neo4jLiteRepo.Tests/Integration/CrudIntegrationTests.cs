using Neo4j.Driver;
using Neo4jLiteRepo.Sample.Nodes;
using NUnit.Framework;

namespace Neo4jLiteRepo.Tests.Integration;

/// <summary>
/// Integration tests for Neo4jGenericRepo CRUD operations.
/// </summary>
[TestFixture]
public class CrudIntegrationTests : Neo4jIntegrationTestBase
{

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
        await Repo.UpsertNode(movie);

        // Assert
        var loaded = await Repo.LoadAsync<Movie>("test-1");
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
        await Repo.UpsertNode(movie);

        // Act - Update the same node
        movie.Title = "Updated Title";
        movie.Released = 2021;
        await Repo.UpsertNode(movie);

        // Assert
        var loaded = await Repo.LoadAsync<Movie>("test-2");
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
        await Repo.UpsertNodes(movies);

        // Assert - Verify all three nodes exist
        var allMovies = await Repo.LoadAllAsync<Movie>();
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
        var loaded = await Repo.LoadAsync<Movie>("non-existent-id");

        // Assert
        Assert.That(loaded, Is.Null);
    }

    [Test]
    public async Task LoadAllAsync_ReturnsAllNodes()
    {
        // Arrange
        await Repo.UpsertNode(new Movie { Id = "load-1", Title = "Movie 1", Released = 2021, Tagline = "Tag 1" });
        await Repo.UpsertNode(new Movie { Id = "load-2", Title = "Movie 2", Released = 2022, Tagline = "Tag 2" });

        // Act
        var movies = await Repo.LoadAllAsync<Movie>();

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
            await Repo.UpsertNode(new Movie { Id = $"page-{i}", Title = $"Movie {i}", Released = 2020 + i, Tagline = $"Tag {i}" });
        }

        // Act - Get page 2 (skip 2, take 2)
        var page = await Repo.LoadAllAsync<Movie>(skip: 2, take: 2);

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
        await Repo.UpsertNode(movie);

        // Verify it exists
        var beforeDelete = await Repo.LoadAsync<Movie>("delete-1");
        Assert.That(beforeDelete, Is.Not.Null);

        // Act
        await Repo.DetachDeleteAsync(movie);

        // Assert
        var afterDelete = await Repo.LoadAsync<Movie>("delete-1");
        Assert.That(afterDelete, Is.Null);
    }

    [Test]
    public async Task DetachDeleteAsync_ByPrimaryKey_DeletesNode()
    {
        // Arrange
        await Repo.UpsertNode(new Movie { Id = "delete-pk", Title = "To Delete", Released = 2023, Tagline = "Delete by PK" });

        // Verify it exists
        var beforeDelete = await Repo.LoadAsync<Movie>("delete-pk");
        Assert.That(beforeDelete, Is.Not.Null);

        // Act
        await Repo.DetachDeleteAsync<Movie>("delete-pk");

        // Assert
        var afterDelete = await Repo.LoadAsync<Movie>("delete-pk");
        Assert.That(afterDelete, Is.Null);
    }

    [Test]
    public async Task DetachDeleteManyAsync_DeletesMultipleNodes()
    {
        // Arrange
        await Repo.UpsertNodes(new List<Movie>
        {
            new() { Id = "del-many-1", Title = "Delete 1", Released = 2021, Tagline = "Tag 1" },
            new() { Id = "del-many-2", Title = "Delete 2", Released = 2022, Tagline = "Tag 2" },
            new() { Id = "del-many-3", Title = "Keep", Released = 2023, Tagline = "Tag 3" }
        });

        // Act - Delete first two
        await Repo.DetachDeleteManyAsync<Movie>(new List<string> { "del-many-1", "del-many-2" });

        // Assert - Verify deleted nodes are gone and kept node remains
        var deleted1 = await Repo.LoadAsync<Movie>("del-many-1");
        var deleted2 = await Repo.LoadAsync<Movie>("del-many-2");
        var kept = await Repo.LoadAsync<Movie>("del-many-3");
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
        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(genre);

        // Act
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);

        // Assert - Load movie and check relationship
        var loadedMovie = await Repo.LoadAsync<Movie>("rel-movie");
        Assert.That(loadedMovie, Is.Not.Null);
        Assert.That(loadedMovie!.GenreIds, Does.Contain("rel-genre"));
    }

    [Test]
    public async Task DeleteRelationshipAsync_RemovesRelationship()
    {
        // Arrange
        var movie = new Movie { Id = "del-rel-movie", Title = "Movie", Released = 2023, Tagline = "Tag" };
        var genre = new Genre { Id = "del-rel-genre", Name = "Comedy" };
        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);

        // Verify relationship exists
        var beforeDelete = await Repo.LoadAsync<Movie>("del-rel-movie");
        Assert.That(beforeDelete, Is.Not.Null);
        Assert.That(beforeDelete!.GenreIds, Does.Contain("del-rel-genre"));
        var genreCountBefore = beforeDelete.GenreIds?.Count() ?? 0;

        // Act
        await Repo.DeleteRelationshipAsync(movie, "IN_GENRE", genre, Neo4jLiteRepo.Models.EdgeDirection.Outgoing);

        // Assert
        var afterDelete = await Repo.LoadAsync<Movie>("del-rel-movie");
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
        await Repo.UpsertNodes(new List<Movie>
        {
            new() { Id = "read-1", Title = "Action Movie", Released = 2021, Tagline = "Tag" },
            new() { Id = "read-2", Title = "Comedy Movie", Released = 2022, Tagline = "Tag" }
        });

        // Act - Filter for movies from this test only
        var results = await Repo.ExecuteReadListAsync<Movie>(
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
        await Repo.UpsertNodes(new List<Movie>
        {
            new() { Id = "count-1", Title = "Movie 1", Released = 2021, Tagline = "Tag" },
            new() { Id = "count-2", Title = "Movie 2", Released = 2022, Tagline = "Tag" },
            new() { Id = "count-3", Title = "Movie 3", Released = 2023, Tagline = "Tag" }
        });

        // Act - Query specifically for the test data
        var count = await Repo.ExecuteReadScalarAsync<long>(
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

        await Repo.UpsertNode(connectedMovie);
        await Repo.UpsertNode(orphanMovie);
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(connectedMovie, "IN_GENRE", genre);

        // Verify both movies exist before cleanup
        var connectedBefore = await Repo.LoadAsync<Movie>("connected");
        var orphanBefore = await Repo.LoadAsync<Movie>("orphan");
        Assert.That(connectedBefore, Is.Not.Null);
        Assert.That(orphanBefore, Is.Not.Null);

        // Act - Remove orphan movies
        var removedCount = await Repo.RemoveOrphansAsync<Movie>();

        // Assert - Orphan should be removed, connected should remain
        Assert.That(removedCount, Is.GreaterThanOrEqualTo(1));
        var connectedAfter = await Repo.LoadAsync<Movie>("connected");
        var orphanAfter = await Repo.LoadAsync<Movie>("orphan");
        Assert.That(connectedAfter, Is.Not.Null);
        Assert.That(orphanAfter, Is.Null);
    }

    #endregion

    #region Cleanup Verification Tests

    [Test]
    public async Task Cleanup_EnsuresDatabaseIsEmpty()
    {
        // Arrange - Add some test data
        await Repo.UpsertNode(new Movie { Id = "cleanup-test", Title = "Test", Released = 2023, Tagline = "Tag" });
        var beforeCleanup = await Repo.LoadAllAsync<Movie>();
        Assert.That(beforeCleanup.Any(m => m.Id == "cleanup-test"), Is.True, "Test data should exist before cleanup");

        // Act - Cleanup using CleanupDatabase method from base class
        await CleanupDatabase();

        // Assert - Database should be completely empty (using direct query)
        var verifySession = Driver.AsyncSession(o => o.WithDatabase(TestDatabase));
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
}
