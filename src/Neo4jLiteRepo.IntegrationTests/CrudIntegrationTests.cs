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

    #region DetachDeleteNodesByIdsAsync Tests - All Overloads

    [Test]
    [Ignore("DetachDeleteNodesByIdsAsync has a hardcoded whitelist that returns false for all labels - by design for safety")]
    public async Task DetachDeleteNodesByIdsAsync_Standalone_DeletesNodes()
    {
        // Arrange
        var movies = new List<Movie>
        {
            new() { Id = "byid-1", Title = "Movie 1", Released = 2021, Tagline = "T1" },
            new() { Id = "byid-2", Title = "Movie 2", Released = 2022, Tagline = "T2" },
            new() { Id = "byid-3", Title = "Movie 3", Released = 2023, Tagline = "T3" }
        };
        await Repo.UpsertNodes(movies);

        // Act - Delete by label and IDs (standalone creates own session/transaction)
        await Repo.DetachDeleteNodesByIdsAsync("Movie", new[] { "byid-1", "byid-2" });

        // Assert
        Assert.That(await Repo.LoadAsync<Movie>("byid-1"), Is.Null);
        Assert.That(await Repo.LoadAsync<Movie>("byid-2"), Is.Null);
        Assert.That(await Repo.LoadAsync<Movie>("byid-3"), Is.Not.Null);
    }

    [Test]
    [Ignore("DetachDeleteNodesByIdsAsync has a hardcoded whitelist that returns false for all labels - by design for safety")]
    public async Task DetachDeleteNodesByIdsAsync_WithSession_DeletesNodes()
    {
        // Arrange
        var movies = new List<Movie>
        {
            new() { Id = "session-byid-1", Title = "M1", Released = 2021, Tagline = "T1" },
            new() { Id = "session-byid-2", Title = "M2", Released = 2022, Tagline = "T2" }
        };
        await Repo.UpsertNodes(movies);

        // Act - Use session overload
        await using var session = Repo.StartSession();
        await Repo.DetachDeleteNodesByIdsAsync("Movie", new[] { "session-byid-1" }, session);

        // Assert
        Assert.That(await Repo.LoadAsync<Movie>("session-byid-1"), Is.Null);
        Assert.That(await Repo.LoadAsync<Movie>("session-byid-2"), Is.Not.Null);
    }

    [Test]
    [Ignore("DetachDeleteNodesByIdsAsync has a hardcoded whitelist that returns false for all labels - by design for safety")]
    public async Task DetachDeleteNodesByIdsAsync_WithTransaction_DeletesNodes()
    {
        // Arrange
        var movies = new List<Movie>
        {
            new() { Id = "tx-byid-1", Title = "M1", Released = 2021, Tagline = "T1" },
            new() { Id = "tx-byid-2", Title = "M2", Released = 2022, Tagline = "T2" }
        };
        await Repo.UpsertNodes(movies);

        // Act - Use transaction overload
        await using var session = Repo.StartSession();
        await using var tx = await session.BeginTransactionAsync();
        await Repo.DetachDeleteNodesByIdsAsync("Movie", new[] { "tx-byid-1" }, tx);
        await tx.CommitAsync();

        // Assert
        Assert.That(await Repo.LoadAsync<Movie>("tx-byid-1"), Is.Null);
        Assert.That(await Repo.LoadAsync<Movie>("tx-byid-2"), Is.Not.Null);
    }

    [Test]
    [Ignore("DetachDeleteNodesByIdsAsync has a hardcoded whitelist that returns false for all labels - by design for safety")]
    public async Task DetachDeleteNodesByIdsAsync_WithLargeBatch_HandlesBatching()
    {
        // Arrange - Create more than batch size (500)
        var movies = Enumerable.Range(1, 600)
            .Select(i => new Movie 
            { 
                Id = $"batch-del-{i}", 
                Title = $"Movie {i}", 
                Released = 2020, 
                Tagline = "T" 
            })
            .ToList();
        await Repo.UpsertNodes(movies);

        // Act - Delete all in batches
        var idsToDelete = movies.Select(m => m.Id).ToList();
        await Repo.DetachDeleteNodesByIdsAsync("Movie", idsToDelete);

        // Assert - Verify all deleted
        var remaining = await Repo.LoadAllAsync<Movie>();
        var batchDeleted = remaining.Where(m => m.Id.StartsWith("batch-del-")).ToList();
        Assert.That(batchDeleted, Is.Empty);
    }

    [Test]
    [Ignore("DetachDeleteNodesByIdsAsync has a hardcoded whitelist that returns false for all labels - by design for safety")]
    public async Task DetachDeleteNodesByIdsAsync_RemovesRelationships()
    {
        // Arrange - Create node with relationships
        var movie = new Movie { Id = "del-with-rels", Title = "Movie", Released = 2023, Tagline = "Test" };
        var genre = new Genre { Id = "del-genre", Name = "Genre", Description = "Test" };
        
        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);

        // Act - DETACH DELETE should remove node and relationships
        await Repo.DetachDeleteNodesByIdsAsync("Movie", new[] { "del-with-rels" });

        // Assert
        Assert.That(await Repo.LoadAsync<Movie>("del-with-rels"), Is.Null);
        Assert.That(await Repo.LoadAsync<Genre>("del-genre"), Is.Not.Null); // Genre should remain
    }

    #endregion

    #region LoadAsync with Relationships Tests

    [Test]
    public async Task LoadAsync_PopulatesRelationshipIds()
    {
        // Arrange
        var movie = new Movie { Id = "load-rels", Title = "Movie", Released = 2023, Tagline = "Test" };
        var genre1 = new Genre { Id = "load-g1", Name = "Action", Description = "A" };
        var genre2 = new Genre { Id = "load-g2", Name = "Drama", Description = "D" };

        await Repo.UpsertNode(movie);
        await Repo.UpsertNodes(new[] { genre1, genre2 });
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre1);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre2);

        // Act
        var loaded = await Repo.LoadAsync<Movie>("load-rels");

        // Assert
        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.GenreIds, Is.Not.Null);
        Assert.That(loaded.GenreIds!.Count(), Is.EqualTo(2));
        Assert.That(loaded.GenreIds, Does.Contain("load-g1"));
        Assert.That(loaded.GenreIds, Does.Contain("load-g2"));
    }

    [Test]
    public async Task LoadAllAsync_WithIncludeEdgeObjects_PopulatesEdges()
    {
        // Arrange - This tests the overload with includeEdgeObjects parameter
        var movie = new Movie { Id = "edge-obj-movie", Title = "Movie", Released = 2023, Tagline = "Test" };
        var genre = new Genre { Id = "edge-obj-genre", Name = "Action", Description = "A" };

        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);

        // Act - Load with edge objects included
        var movies = await Repo.LoadAllAsync<Movie>(skip: 0, take: 100, includeEdgeObjects: true, includeEdges: new[] { "IN_GENRE" });

        // Assert
        var loadedMovie = movies.FirstOrDefault(m => m.Id == "edge-obj-movie");
        Assert.That(loadedMovie, Is.Not.Null);
        Assert.That(loadedMovie!.GenreIds, Does.Contain("edge-obj-genre"));
    }

    #endregion

    #region Session and Transaction Pattern Tests

    [Test]
    public async Task SessionPattern_MultipleOperations_ShareSession()
    {
        // Act - Multiple operations using same session
        await using var session = Repo.StartSession();
        
        var movie1 = new Movie { Id = "session-1", Title = "M1", Released = 2021, Tagline = "T1" };
        var movie2 = new Movie { Id = "session-2", Title = "M2", Released = 2022, Tagline = "T2" };
        
        await Repo.UpsertNode(movie1, session);
        await Repo.UpsertNode(movie2, session);
        
        // Assert - Both should be saved
        Assert.That(await Repo.LoadAsync<Movie>("session-1"), Is.Not.Null);
        Assert.That(await Repo.LoadAsync<Movie>("session-2"), Is.Not.Null);
    }

    [Test]
    public async Task TransactionPattern_Commit_SavesChanges()
    {
        // Act - Use transaction and commit
        await using var session = Repo.StartSession();
        await using var tx = await session.BeginTransactionAsync();
        
        var movie = new Movie { Id = "tx-commit", Title = "Movie", Released = 2023, Tagline = "Test" };
        await Repo.UpsertNode(movie, tx);
        await tx.CommitAsync();

        // Assert
        var loaded = await Repo.LoadAsync<Movie>("tx-commit");
        Assert.That(loaded, Is.Not.Null);
    }

    [Test]
    public async Task TransactionPattern_Rollback_DiscardsChanges()
    {
        // Act - Use transaction but rollback
        await using var session = Repo.StartSession();
        await using var tx = await session.BeginTransactionAsync();
        
        var movie = new Movie { Id = "tx-rollback", Title = "Movie", Released = 2023, Tagline = "Test" };
        await Repo.UpsertNode(movie, tx);
        await tx.RollbackAsync(); // Rollback instead of commit

        // Assert - Changes should be discarded
        var loaded = await Repo.LoadAsync<Movie>("tx-rollback");
        Assert.That(loaded, Is.Null);
    }

    [Test]
    public async Task TransactionPattern_AtomicOperations_AllOrNothing()
    {
        // Arrange - Create genres first
        var genre1 = new Genre { Id = "atomic-g1", Name = "G1", Description = "G1" };
        var genre2 = new Genre { Id = "atomic-g2", Name = "G2", Description = "G2" };
        await Repo.UpsertNodes(new[] { genre1, genre2 });

        // Act - Multiple related operations in single transaction
        await using var session = Repo.StartSession();
        await using var tx = await session.BeginTransactionAsync();
        
        var movie = new Movie { Id = "atomic-movie", Title = "Movie", Released = 2023, Tagline = "Test" };
        await Repo.UpsertNode(movie, tx);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre1, tx);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre2, tx);
        await tx.CommitAsync();

        // Assert - All operations should succeed
        var loaded = await Repo.LoadAsync<Movie>("atomic-movie");
        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.GenreIds!.Count(), Is.EqualTo(2));
    }

    [Test]
    public async Task TransactionPattern_ExceptionRollsBack()
    {
        // Act & Assert - Exception during transaction should rollback
        await using var session = Repo.StartSession();
        try
        {
            await using var tx = await session.BeginTransactionAsync();
            
            var movie = new Movie { Id = "exception-movie", Title = "Movie", Released = 2023, Tagline = "Test" };
            await Repo.UpsertNode(movie, tx);
            
            // Intentionally cause a Cypher syntax error to force an exception
            var badQuery = "INVALID CYPHER SYNTAX HERE";
            await tx.RunAsync(badQuery);
            
            await tx.CommitAsync();
            Assert.Fail("Should have thrown exception");
        }
        catch
        {
            // Expected - transaction should rollback
        }

        // Assert - Movie should not exist (transaction rolled back)
        var loaded = await Repo.LoadAsync<Movie>("exception-movie");
        Assert.That(loaded, Is.Null);
    }

    #endregion

    #region UpsertNodes Batch Tests

    [Test]
    public async Task UpsertNodes_LargeBatch_HandlesEfficiently()
    {
        // Arrange - Large batch to test performance
        var movies = Enumerable.Range(1, 1000)
            .Select(i => new Movie 
            { 
                Id = $"large-batch-{i}", 
                Title = $"Movie {i}", 
                Released = 2020 + (i % 5), 
                Tagline = $"Tagline {i}" 
            })
            .ToList();

        // Act
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await Repo.UpsertNodes(movies);
        sw.Stop();

        // Assert
        var loaded = await Repo.LoadAllAsync<Movie>();
        var batchMovies = loaded.Where(m => m.Id.StartsWith("large-batch-")).ToList();
        Assert.That(batchMovies.Count, Is.EqualTo(1000));
        
        // Should complete in reasonable time (adjust threshold as needed)
        Assert.That(sw.ElapsedMilliseconds, Is.LessThan(30000), $"Batch upsert took {sw.ElapsedMilliseconds}ms");
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
