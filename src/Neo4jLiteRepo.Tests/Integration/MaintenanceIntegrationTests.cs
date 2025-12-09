using Neo4jLiteRepo.Sample.Nodes;
using NUnit.Framework;

namespace Neo4jLiteRepo.Tests.Integration;

/// <summary>
/// Integration tests for Neo4jGenericRepo maintenance operations.
/// Tests methods in Neo4jGenericRepo.Maintenance.cs
/// Uses dedicated IntegrationTests database that gets wiped during teardown.
/// </summary>
[TestFixture]
public class MaintenanceIntegrationTests : Neo4jIntegrationTestBase
{
    [SetUp]
    public async Task SetUp()
    {
        await CleanupDatabase();
    }

    [TearDown]
    public async Task TearDown()
    {
        await CleanupDatabase();
    }

    #region RemoveOrphansAsync Tests - Standalone (creates own session/transaction)

    [Test]
    public async Task RemoveOrphansAsync_Standalone_RemovesNodesWithNoRelationships()
    {
        // Arrange - Create orphan and connected nodes
        var orphan1 = new Movie { Id = "orphan-1", Title = "Orphan 1", Released = 2020, Tagline = "Alone" };
        var orphan2 = new Movie { Id = "orphan-2", Title = "Orphan 2", Released = 2021, Tagline = "Alone" };
        var connected = new Movie { Id = "connected", Title = "Connected", Released = 2022, Tagline = "Has links" };
        var genre = new Genre { Id = "test-genre", Name = "Test", Description = "Test" };

        await Repo.UpsertNodes(new[] { orphan1, orphan2, connected });
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(connected, "IN_GENRE", genre);

        // Verify all exist
        var allBefore = await Repo.LoadAllAsync<Movie>();
        Assert.That(allBefore.Count, Is.GreaterThanOrEqualTo(3));

        // Act - Use standalone method (creates own session/transaction)
        var removedCount = await Repo.RemoveOrphansAsync<Movie>();

        // Assert
        Assert.That(removedCount, Is.EqualTo(2));
        var orphan1After = await Repo.LoadAsync<Movie>("orphan-1");
        var orphan2After = await Repo.LoadAsync<Movie>("orphan-2");
        var connectedAfter = await Repo.LoadAsync<Movie>("connected");
        
        Assert.That(orphan1After, Is.Null);
        Assert.That(orphan2After, Is.Null);
        Assert.That(connectedAfter, Is.Not.Null);
    }

    [Test]
    public async Task RemoveOrphansAsync_Standalone_WithNoOrphans_ReturnsZero()
    {
        // Arrange - Create only connected nodes
        var movie1 = new Movie { Id = "movie-1", Title = "Movie 1", Released = 2021, Tagline = "T1" };
        var movie2 = new Movie { Id = "movie-2", Title = "Movie 2", Released = 2022, Tagline = "T2" };
        var genre = new Genre { Id = "genre", Name = "Genre", Description = "G" };

        await Repo.UpsertNodes(new[] { movie1, movie2 });
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(movie1, "IN_GENRE", genre);
        await Repo.MergeRelationshipAsync(movie2, "IN_GENRE", genre);

        // Act
        var removedCount = await Repo.RemoveOrphansAsync<Movie>();

        // Assert
        Assert.That(removedCount, Is.EqualTo(0));
        var movie1After = await Repo.LoadAsync<Movie>("movie-1");
        var movie2After = await Repo.LoadAsync<Movie>("movie-2");
        Assert.That(movie1After, Is.Not.Null);
        Assert.That(movie2After, Is.Not.Null);
    }

    #endregion

    #region RemoveOrphansAsync Tests - Session Overload

    [Test]
    public async Task RemoveOrphansAsync_WithSession_RemovesOrphans()
    {
        // Arrange
        var orphan = new Movie { Id = "session-orphan", Title = "Orphan", Released = 2020, Tagline = "Alone" };
        var connected = new Movie { Id = "session-connected", Title = "Connected", Released = 2021, Tagline = "Linked" };
        var genre = new Genre { Id = "session-genre", Name = "Genre", Description = "G" };

        await Repo.UpsertNodes(new[] { orphan, connected });
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(connected, "IN_GENRE", genre);

        // Act - Use session overload
        await using var session = Repo.StartSession();
        var removedCount = await Repo.RemoveOrphansAsync<Movie>(session);

        // Assert
        Assert.That(removedCount, Is.EqualTo(1));
        var orphanAfter = await Repo.LoadAsync<Movie>("session-orphan");
        var connectedAfter = await Repo.LoadAsync<Movie>("session-connected");
        Assert.That(orphanAfter, Is.Null);
        Assert.That(connectedAfter, Is.Not.Null);
    }

    [Test]
    public async Task RemoveOrphansAsync_WithSession_MultipleOperations_SharesSession()
    {
        // Arrange - Create orphans for two different node types
        var orphanMovie = new Movie { Id = "multi-orphan-movie", Title = "Orphan Movie", Released = 2020, Tagline = "Alone" };
        var orphanGenre = new Genre { Id = "multi-orphan-genre", Name = "Orphan Genre", Description = "Alone" };
        var connectedMovie = new Movie { Id = "multi-connected-movie", Title = "Connected", Released = 2021, Tagline = "Linked" };
        var connectedGenre = new Genre { Id = "multi-connected-genre", Name = "Connected Genre", Description = "Linked" };

        await Repo.UpsertNodes(new[] { orphanMovie, connectedMovie });
        await Repo.UpsertNodes(new[] { orphanGenre, connectedGenre });
        await Repo.MergeRelationshipAsync(connectedMovie, "IN_GENRE", connectedGenre);

        // Act - Multiple cleanup operations using same session
        await using var session = Repo.StartSession();
        var moviesRemoved = await Repo.RemoveOrphansAsync<Movie>(session);
        var genresRemoved = await Repo.RemoveOrphansAsync<Genre>(session);

        // Assert
        Assert.That(moviesRemoved, Is.EqualTo(1));
        Assert.That(genresRemoved, Is.EqualTo(1));
        
        Assert.That(await Repo.LoadAsync<Movie>("multi-orphan-movie"), Is.Null);
        Assert.That(await Repo.LoadAsync<Genre>("multi-orphan-genre"), Is.Null);
        Assert.That(await Repo.LoadAsync<Movie>("multi-connected-movie"), Is.Not.Null);
        Assert.That(await Repo.LoadAsync<Genre>("multi-connected-genre"), Is.Not.Null);
    }

    #endregion

    #region RemoveOrphansAsync Tests - Transaction Overload

    [Test]
    public async Task RemoveOrphansAsync_WithTransaction_RemovesOrphans()
    {
        // Arrange
        var orphan = new Movie { Id = "tx-orphan", Title = "Orphan", Released = 2020, Tagline = "Alone" };
        var connected = new Movie { Id = "tx-connected", Title = "Connected", Released = 2021, Tagline = "Linked" };
        var genre = new Genre { Id = "tx-genre", Name = "Genre", Description = "G" };

        await Repo.UpsertNodes(new[] { orphan, connected });
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(connected, "IN_GENRE", genre);

        // Act - Use transaction overload for full control
        await using var session = Repo.StartSession();
        await using var tx = await session.BeginTransactionAsync();
        var removedCount = await Repo.RemoveOrphansAsync<Movie>(tx);
        await tx.CommitAsync();

        // Assert
        Assert.That(removedCount, Is.EqualTo(1));
        var orphanAfter = await Repo.LoadAsync<Movie>("tx-orphan");
        var connectedAfter = await Repo.LoadAsync<Movie>("tx-connected");
        Assert.That(orphanAfter, Is.Null);
        Assert.That(connectedAfter, Is.Not.Null);
    }

    [Test]
    public async Task RemoveOrphansAsync_WithTransaction_Rollback_PreservesOrphans()
    {
        // Arrange
        var orphan = new Movie { Id = "rollback-orphan", Title = "Orphan", Released = 2020, Tagline = "Alone" };
        await Repo.UpsertNode(orphan);

        // Act - Remove orphans but rollback
        await using var session = Repo.StartSession();
        await using var tx = await session.BeginTransactionAsync();
        var removedCount = await Repo.RemoveOrphansAsync<Movie>(tx);
        await tx.RollbackAsync(); // Rollback instead of commit

        // Assert - Orphan should still exist
        Assert.That(removedCount, Is.EqualTo(1));
        var orphanAfter = await Repo.LoadAsync<Movie>("rollback-orphan");
        Assert.That(orphanAfter, Is.Not.Null, "Orphan should still exist after rollback");
    }

    [Test]
    public async Task RemoveOrphansAsync_WithTransaction_AtomicOperations()
    {
        // Arrange - Create orphans and perform multiple operations in single transaction
        var orphanMovie = new Movie { Id = "atomic-orphan", Title = "Orphan", Released = 2020, Tagline = "Alone" };
        var newMovie = new Movie { Id = "atomic-new", Title = "New", Released = 2021, Tagline = "New" };
        var genre = new Genre { Id = "atomic-genre", Name = "Genre", Description = "G" };

        await Repo.UpsertNode(orphanMovie);
        await Repo.UpsertNode(genre);

        // Act - All operations in single transaction
        await using var session = Repo.StartSession();
        await using var tx = await session.BeginTransactionAsync();
        
        // Add new movie with relationship
        await Repo.UpsertNode(newMovie, tx);
        await Repo.MergeRelationshipAsync(newMovie, "IN_GENRE", genre, tx);
        
        // Remove orphans (should remove orphanMovie but not newMovie)
        var removedCount = await Repo.RemoveOrphansAsync<Movie>(tx);
        
        await tx.CommitAsync();

        // Assert
        Assert.That(removedCount, Is.EqualTo(1));
        Assert.That(await Repo.LoadAsync<Movie>("atomic-orphan"), Is.Null);
        Assert.That(await Repo.LoadAsync<Movie>("atomic-new"), Is.Not.Null);
    }

    #endregion

    #region RemoveOrphansAsync Tests - Large Batches

    [Test]
    public async Task RemoveOrphansAsync_WithLargeNumberOfOrphans_HandlesBatching()
    {
        // Arrange - Create more orphans than batch size (batch size is 400)
        var orphans = Enumerable.Range(1, 450)
            .Select(i => new Movie 
            { 
                Id = $"batch-orphan-{i}", 
                Title = $"Orphan {i}", 
                Released = 2020, 
                Tagline = "Alone" 
            })
            .ToList();
        
        var connected = new Movie { Id = "batch-connected", Title = "Connected", Released = 2021, Tagline = "Linked" };
        var genre = new Genre { Id = "batch-genre", Name = "Genre", Description = "G" };

        await Repo.UpsertNodes(orphans);
        await Repo.UpsertNode(connected);
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(connected, "IN_GENRE", genre);

        // Act
        var removedCount = await Repo.RemoveOrphansAsync<Movie>();

        // Assert - Should remove all 450 orphans in batches
        Assert.That(removedCount, Is.EqualTo(450));
        
        // Verify a sample of orphans are gone
        Assert.That(await Repo.LoadAsync<Movie>("batch-orphan-1"), Is.Null);
        Assert.That(await Repo.LoadAsync<Movie>("batch-orphan-450"), Is.Null);
        Assert.That(await Repo.LoadAsync<Movie>("batch-connected"), Is.Not.Null);
    }

    #endregion

    #region RemoveOrphansAsync Tests - Edge Cases

    [Test]
    public async Task RemoveOrphansAsync_WithEmptyDatabase_ReturnsZero()
    {
        // Act - Clean database should have no orphans
        var removedCount = await Repo.RemoveOrphansAsync<Movie>();

        // Assert
        Assert.That(removedCount, Is.EqualTo(0));
    }

    [Test]
    public async Task RemoveOrphansAsync_NodeWithSelfRelationship_IsNotOrphan()
    {
        // Arrange - Create node with self-relationship
        var movie = new Movie { Id = "self-rel", Title = "Self Related", Released = 2020, Tagline = "Narcissistic" };
        await Repo.UpsertNode(movie);
        
        // Create self-relationship using direct Cypher (not a typical use case but tests edge case)
        await using var session = Repo.StartSession();
        await session.ExecuteWriteAsync(async tx =>
        {
            await tx.RunAsync(
                "MATCH (m:Movie {id: $id}) CREATE (m)-[:SEQUEL_TO]->(m)",
                new { id = "self-rel" });
        });

        // Act
        var removedCount = await Repo.RemoveOrphansAsync<Movie>();

        // Assert - Node with self-relationship should not be considered orphan
        Assert.That(removedCount, Is.EqualTo(0));
        Assert.That(await Repo.LoadAsync<Movie>("self-rel"), Is.Not.Null);
    }

    [Test]
    public async Task RemoveOrphansAsync_OnlyRemovesSpecifiedNodeType()
    {
        // Arrange - Create orphans of different types
        var orphanMovie = new Movie { Id = "type-orphan-movie", Title = "Orphan Movie", Released = 2020, Tagline = "Alone" };
        var orphanGenre = new Genre { Id = "type-orphan-genre", Name = "Orphan Genre", Description = "Alone" };
        
        await Repo.UpsertNode(orphanMovie);
        await Repo.UpsertNode(orphanGenre);

        // Act - Remove only Movie orphans
        var removedCount = await Repo.RemoveOrphansAsync<Movie>();

        // Assert - Only movies should be removed, genre should remain
        Assert.That(removedCount, Is.EqualTo(1));
        Assert.That(await Repo.LoadAsync<Movie>("type-orphan-movie"), Is.Null);
        Assert.That(await Repo.LoadAsync<Genre>("type-orphan-genre"), Is.Not.Null);
    }

    #endregion
}
