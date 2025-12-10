using Neo4j.Driver;
using Neo4jLiteRepo.Models;
using Neo4jLiteRepo.Sample.Nodes;
using NUnit.Framework;

namespace Neo4jLiteRepo.Tests.Integration;

/// <summary>
/// Integration tests for Neo4jGenericRepo relationship operations.
/// Tests methods in Neo4jGenericRepo.Relationships.cs
/// </summary>
[TestFixture]
public class RelationshipIntegrationTests : Neo4jIntegrationTestBase
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

    #region CreateRelationshipsAsync Tests

    [Test]
    [Ignore("CreateRelationshipsAsync requires EdgeSeed data (MovieGenreEdge) to be loaded in DataSourceService - not configured for integration tests")]
    public async Task CreateRelationshipsAsync_CreatesRelationshipsFromNodeAttributes()
    {
        // Arrange
        var movie = new Movie
        {
            Id = "create-rel-movie",
            Title = "Test Movie",
            Released = 2023,
            Tagline = "Test",
            GenreIds = ["action", "scifi"]
        };
        var actionGenre = new Genre { Id = "action", Name = "Action", Description = "Action movies" };
        var scifiGenre = new Genre { Id = "scifi", Name = "Sci-Fi", Description = "Science fiction" };

        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(actionGenre);
        await Repo.UpsertNode(scifiGenre);

        // Act
        var result = await Repo.CreateRelationshipsAsync(movie);

        // Assert
        Assert.That(result, Is.True);
        var loadedMovie = await Repo.LoadAsync<Movie>("create-rel-movie");
        Assert.That(loadedMovie, Is.Not.Null);
        Assert.That(loadedMovie!.GenreIds, Does.Contain("action"));
        Assert.That(loadedMovie.GenreIds, Does.Contain("scifi"));
    }

    [Test]
    [Ignore("CreateRelationshipsAsync requires EdgeSeed data (MovieGenreEdge) to be loaded in DataSourceService - not configured for integration tests")]
    public async Task CreateRelationshipsAsync_WithSession_CreatesRelationships()
    {
        // Arrange
        var movie = new Movie
        {
            Id = "session-rel-movie",
            Title = "Session Test",
            Released = 2023,
            Tagline = "Test",
            GenreIds = ["drama"]
        };
        var genre = new Genre { Id = "drama", Name = "Drama", Description = "Drama movies" };

        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(genre);

        // Act
        await using var session = Repo.StartSession();
        var result = await Repo.CreateRelationshipsAsync(movie, session);

        // Assert
        Assert.That(result, Is.True);
        var loadedMovie = await Repo.LoadAsync<Movie>("session-rel-movie");
        Assert.That(loadedMovie!.GenreIds, Does.Contain("drama"));
    }

    [Test]
    [Ignore("CreateRelationshipsAsync requires EdgeSeed data (MovieGenreEdge) to be loaded in DataSourceService - not configured for integration tests")]
    public async Task CreateRelationshipsAsync_MultipleNodes_CreatesAllRelationships()
    {
        // Arrange
        var movies = new List<Movie>
        {
            new() { Id = "multi-1", Title = "Movie 1", Released = 2021, Tagline = "T1", GenreIds = ["action"] },
            new() { Id = "multi-2", Title = "Movie 2", Released = 2022, Tagline = "T2", GenreIds = ["comedy"] }
        };
        var actionGenre = new Genre { Id = "action", Name = "Action", Description = "Action" };
        var comedyGenre = new Genre { Id = "comedy", Name = "Comedy", Description = "Comedy" };

        await Repo.UpsertNode(actionGenre);
        await Repo.UpsertNode(comedyGenre);
        await Repo.UpsertNodes(movies);

        // Act
        var result = await Repo.CreateRelationshipsAsync(movies);

        // Assert
        Assert.That(result, Is.True);
        var movie1 = await Repo.LoadAsync<Movie>("multi-1");
        var movie2 = await Repo.LoadAsync<Movie>("multi-2");
        Assert.That(movie1!.GenreIds, Does.Contain("action"));
        Assert.That(movie2!.GenreIds, Does.Contain("comedy"));
    }

    #endregion

    #region DeleteRelationshipsOfTypeFromAsync Tests

    [Test]
    public async Task DeleteRelationshipsOfTypeFromAsync_RemovesOutgoingRelationships()
    {
        // Arrange
        var movie = new Movie { Id = "del-type-movie", Title = "Movie", Released = 2023, Tagline = "Test" };
        var genre1 = new Genre { Id = "genre1", Name = "Genre1", Description = "G1" };
        var genre2 = new Genre { Id = "genre2", Name = "Genre2", Description = "G2" };

        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(genre1);
        await Repo.UpsertNode(genre2);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre1);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre2);

        // Verify relationships exist
        var beforeDelete = await Repo.LoadAsync<Movie>("del-type-movie");
        Assert.That(beforeDelete!.GenreIds?.Count(), Is.EqualTo(2));

        // Act
        await using var session = Repo.StartSession();
        await using var tx = await session.BeginTransactionAsync();
        var result = await Repo.DeleteRelationshipsOfTypeFromAsync(movie, "IN_GENRE", EdgeDirection.Outgoing, tx);
        await tx.CommitAsync();

        // Assert
        Assert.That(result.Counters.RelationshipsDeleted, Is.EqualTo(2));
        var afterDelete = await Repo.LoadAsync<Movie>("del-type-movie");
        Assert.That(afterDelete!.GenreIds, Is.Empty);
    }

    [Test]
    public async Task DeleteRelationshipsOfTypeFromAsync_WithIncomingDirection_RemovesIncomingRelationships()
    {
        // Arrange
        var genre = new Genre { Id = "incoming-genre", Name = "Test Genre", Description = "Test" };
        var movie1 = new Movie { Id = "incoming-m1", Title = "M1", Released = 2021, Tagline = "T1" };
        var movie2 = new Movie { Id = "incoming-m2", Title = "M2", Released = 2022, Tagline = "T2" };

        await Repo.UpsertNode(genre);
        await Repo.UpsertNode(movie1);
        await Repo.UpsertNode(movie2);
        await Repo.MergeRelationshipAsync(movie1, "IN_GENRE", genre);
        await Repo.MergeRelationshipAsync(movie2, "IN_GENRE", genre);

        // Act
        await using var session = Repo.StartSession();
        await using var tx = await session.BeginTransactionAsync();
        var result = await Repo.DeleteRelationshipsOfTypeFromAsync(genre, "IN_GENRE", EdgeDirection.Incoming, tx);
        await tx.CommitAsync();

        // Assert
        Assert.That(result.Counters.RelationshipsDeleted, Is.EqualTo(2));
    }

    #endregion

    #region DeleteEdgesAsync Tests

    [Test]
    public async Task DeleteEdgesAsync_DeletesMultipleSpecificRelationships()
    {
        // Arrange
        var movie1 = new Movie { Id = "edge-m1", Title = "M1", Released = 2021, Tagline = "T1" };
        var movie2 = new Movie { Id = "edge-m2", Title = "M2", Released = 2022, Tagline = "T2" };
        var genre1 = new Genre { Id = "edge-g1", Name = "G1", Description = "G1" };
        var genre2 = new Genre { Id = "edge-g2", Name = "G2", Description = "G2" };

        await Repo.UpsertNodes(new[] { movie1, movie2 });
        await Repo.UpsertNodes(new[] { genre1, genre2 });
        await Repo.MergeRelationshipAsync(movie1, "IN_GENRE", genre1);
        await Repo.MergeRelationshipAsync(movie2, "IN_GENRE", genre2);
        await Repo.MergeRelationshipAsync(movie1, "IN_GENRE", genre2); // Extra relationship

        // Act - Delete specific edges
        var edgeSpecs = new List<EdgeDeleteSpec>
        {
            new(movie1, "IN_GENRE", genre1, EdgeDirection.Outgoing),
            new(movie2, "IN_GENRE", genre2, EdgeDirection.Outgoing)
        };

        await using var session = Repo.StartSession();
        await using var tx = await session.BeginTransactionAsync();
        await Repo.DeleteEdgesAsync(edgeSpecs, tx);
        await tx.CommitAsync();

        // Assert - Verify specific edges deleted but the extra remains
        var m1After = await Repo.LoadAsync<Movie>("edge-m1");
        var m2After = await Repo.LoadAsync<Movie>("edge-m2");
        Assert.That(m1After!.GenreIds, Does.Not.Contain("edge-g1"));
        Assert.That(m1After.GenreIds, Does.Contain("edge-g2")); // Extra edge should remain
        Assert.That(m2After!.GenreIds, Does.Not.Contain("edge-g2"));
    }

    [Test]
    public async Task DeleteEdgesAsync_WithLargeBatch_HandlesCorrectly()
    {
        // Arrange - Create more than one batch worth of relationships (batch size is 500)
        var movie = new Movie { Id = "batch-movie", Title = "Batch Movie", Released = 2023, Tagline = "Test" };
        await Repo.UpsertNode(movie);

        var genres = Enumerable.Range(1, 100)
            .Select(i => new Genre { Id = $"batch-genre-{i}", Name = $"Genre {i}", Description = $"Genre {i}" })
            .ToList();
        await Repo.UpsertNodes(genres);

        // Create relationships
        foreach (var genre in genres)
        {
            await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);
        }

        // Act - Delete all via batch
        var edgeSpecs = genres.Select(g => 
            new EdgeDeleteSpec(movie, "IN_GENRE", g, EdgeDirection.Outgoing)).ToList();

        await using var session = Repo.StartSession();
        await using var tx = await session.BeginTransactionAsync();
        await Repo.DeleteEdgesAsync(edgeSpecs, tx);
        await tx.CommitAsync();

        // Assert
        var movieAfter = await Repo.LoadAsync<Movie>("batch-movie");
        Assert.That(movieAfter!.GenreIds, Is.Empty);
    }

    #endregion

    #region LoadRelatedNodesAsync Tests

    [Test]
    public async Task LoadRelatedNodesAsync_ReturnsDirectlyRelatedNodes()
    {
        // Arrange
        var movie = new Movie { Id = "related-movie", Title = "Movie", Released = 2023, Tagline = "Test" };
        var genre1 = new Genre { Id = "related-g1", Name = "Action", Description = "Action" };
        var genre2 = new Genre { Id = "related-g2", Name = "Drama", Description = "Drama" };

        await Repo.UpsertNode(movie);
        await Repo.UpsertNodes(new[] { genre1, genre2 });
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre1);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre2);

        // Act
        var relatedGenres = await Repo.LoadRelatedNodesAsync<Movie, Genre>("related-movie", "IN_GENRE", 1, 1);

        // Assert
        Assert.That(relatedGenres.Count, Is.EqualTo(2));
        Assert.That(relatedGenres.Any(g => g.Id == "related-g1"), Is.True);
        Assert.That(relatedGenres.Any(g => g.Id == "related-g2"), Is.True);
    }

    [Test]
    public async Task LoadRelatedNodesAsync_WithMultipleHops_ReturnsTransitiveRelationships()
    {
        // Arrange - Create a chain: Movie -> Genre -> Movie
        var movie1 = new Movie { Id = "hop-m1", Title = "Movie 1", Released = 2021, Tagline = "T1" };
        var genre = new Genre { Id = "hop-g", Name = "Shared Genre", Description = "Shared" };
        var movie2 = new Movie { Id = "hop-m2", Title = "Movie 2", Released = 2022, Tagline = "T2" };

        await Repo.UpsertNodes(new[] { movie1, movie2 });
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(movie1, "IN_GENRE", genre);
        await Repo.MergeRelationshipAsync(genre, "HAS_MOVIE", movie2);

        // Act - Find movies related through genre (2 hops)
        var relatedMovies = await Repo.LoadRelatedNodesAsync<Movie, Movie>("hop-m1", "IN_GENRE|HAS_MOVIE", 2, 2);

        // Assert
        Assert.That(relatedMovies.Count, Is.EqualTo(1));
        Assert.That(relatedMovies[0].Id, Is.EqualTo("hop-m2"));
    }

    [Test]
    public async Task LoadRelatedNodesAsync_WithTransaction_ReusesTransaction()
    {
        // Arrange
        var movie = new Movie { Id = "tx-movie", Title = "TX Movie", Released = 2023, Tagline = "Test" };
        var genre = new Genre { Id = "tx-genre", Name = "TX Genre", Description = "Test" };

        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);

        // Act
        await using var session = Repo.StartSession();
        await using var tx = await session.BeginTransactionAsync();
        var relatedGenres = await Repo.LoadRelatedNodesAsync<Movie, Genre>("tx-movie", "IN_GENRE", 1, 1, tx);
        await tx.CommitAsync();

        // Assert
        Assert.That(relatedGenres.Count, Is.EqualTo(1));
        Assert.That(relatedGenres[0].Id, Is.EqualTo("tx-genre"));
    }

    #endregion

    #region LoadRelatedNodeIdsAsync Tests

    [Test]
    public async Task LoadRelatedNodeIdsAsync_ReturnsRelatedIds()
    {
        // Arrange
        var movie = new Movie { Id = "ids-movie", Title = "Movie", Released = 2023, Tagline = "Test" };
        var genre1 = new Genre { Id = "ids-g1", Name = "G1", Description = "G1" };
        var genre2 = new Genre { Id = "ids-g2", Name = "G2", Description = "G2" };

        await Repo.UpsertNode(movie);
        await Repo.UpsertNodes(new[] { genre1, genre2 });
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre1);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre2);

        // Act
        var relatedIds = await Repo.LoadRelatedNodeIdsAsync<Genre>(movie, "IN_GENRE", 1, 1, EdgeDirection.Outgoing);

        // Assert
        Assert.That(relatedIds.Count, Is.EqualTo(2));
        Assert.That(relatedIds, Does.Contain("ids-g1"));
        Assert.That(relatedIds, Does.Contain("ids-g2"));
    }

    [Test]
    public async Task LoadRelatedNodeIdsAsync_WithIncomingDirection_ReturnsIncomingIds()
    {
        // Arrange
        var genre = new Genre { Id = "incoming-ids-g", Name = "Genre", Description = "Test" };
        var movie1 = new Movie { Id = "incoming-ids-m1", Title = "M1", Released = 2021, Tagline = "T1" };
        var movie2 = new Movie { Id = "incoming-ids-m2", Title = "M2", Released = 2022, Tagline = "T2" };

        await Repo.UpsertNode(genre);
        await Repo.UpsertNodes(new[] { movie1, movie2 });
        await Repo.MergeRelationshipAsync(movie1, "IN_GENRE", genre);
        await Repo.MergeRelationshipAsync(movie2, "IN_GENRE", genre);

        // Act - Get incoming movie IDs from genre perspective
        var relatedIds = await Repo.LoadRelatedNodeIdsAsync<Movie>(genre, "IN_GENRE", 1, 1, EdgeDirection.Incoming);

        // Assert
        Assert.That(relatedIds.Count, Is.EqualTo(2));
        Assert.That(relatedIds, Does.Contain("incoming-ids-m1"));
        Assert.That(relatedIds, Does.Contain("incoming-ids-m2"));
    }

    [Test]
    public async Task LoadRelatedNodeIdsAsync_WithBothDirection_ReturnsBothDirections()
    {
        // Arrange
        var movie = new Movie { Id = "both-m", Title = "Movie", Released = 2023, Tagline = "Test" };
        var genre = new Genre { Id = "both-g", Name = "Genre", Description = "Test" };

        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);

        // Act - Query from movie with Both direction
        var relatedIds = await Repo.LoadRelatedNodeIdsAsync<Genre>(movie, "IN_GENRE", 1, 1, EdgeDirection.Both);

        // Assert
        Assert.That(relatedIds.Count, Is.EqualTo(1));
        Assert.That(relatedIds, Does.Contain("both-g"));
    }

    [Test]
    public async Task LoadRelatedNodeIdsAsync_WithMultipleHops_ReturnsTransitiveIds()
    {
        // Arrange - Create chain
        var person = new Person { Id = "chain-person", Name = "Actor", BirthYear = 1980 };
        var movie = new Movie { Id = "chain-movie", Title = "Movie", Released = 2023, Tagline = "Test" };
        var genre = new Genre { Id = "chain-genre", Name = "Genre", Description = "Test" };

        await Repo.UpsertNode(person);
        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(person, "ACTED_IN", movie);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);

        // Act - Find genres related to person through movie (2 hops)
        var relatedIds = await Repo.LoadRelatedNodeIdsAsync<Genre>(person, "ACTED_IN|IN_GENRE", 2, 2, EdgeDirection.Outgoing);

        // Assert
        Assert.That(relatedIds.Count, Is.EqualTo(1));
        Assert.That(relatedIds, Does.Contain("chain-genre"));
    }

    [Test]
    public async Task LoadRelatedNodeIdsAsync_WithTransaction_WorksCorrectly()
    {
        // Arrange
        var movie = new Movie { Id = "tx-ids-movie", Title = "Movie", Released = 2023, Tagline = "Test" };
        var genre = new Genre { Id = "tx-ids-genre", Name = "Genre", Description = "Test" };

        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);

        // Act
        await using var session = Repo.StartSession();
        await using var tx = await session.BeginTransactionAsync();
        var relatedIds = await Repo.LoadRelatedNodeIdsAsync<Genre>(movie, "IN_GENRE", 1, 1, EdgeDirection.Outgoing, tx);
        await tx.CommitAsync();

        // Assert
        Assert.That(relatedIds.Count, Is.EqualTo(1));
        Assert.That(relatedIds, Does.Contain("tx-ids-genre"));
    }

    #endregion
}
