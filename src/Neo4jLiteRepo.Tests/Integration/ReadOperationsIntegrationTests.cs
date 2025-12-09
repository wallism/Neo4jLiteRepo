using Neo4jLiteRepo.Exceptions;
using Neo4jLiteRepo.Sample.Nodes;
using NUnit.Framework;

namespace Neo4jLiteRepo.Tests.Integration;

/// <summary>
/// Integration tests for Neo4jGenericRepo read operations.
/// Tests methods in Neo4jGenericRepo.Read.cs
/// Uses dedicated IntegrationTests database that gets wiped during teardown.
/// </summary>
[TestFixture]
public class ReadOperationsIntegrationTests : Neo4jIntegrationTestBase
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

    #region ExecuteReadStreamAsync Tests

    [Test]
    public async Task ExecuteReadStreamAsync_StreamsLargeResultSet()
    {
        // Arrange - Create more items than typical batch size
        var movies = Enumerable.Range(1, 100)
            .Select(i => new Movie 
            { 
                Id = $"stream-movie-{i}", 
                Title = $"Movie {i}", 
                Released = 2020 + (i % 5), 
                Tagline = $"Tagline {i}" 
            })
            .ToList();
        await Repo.UpsertNodes(movies);

        // Act - Stream results instead of loading all at once
        var streamedMovies = new List<Movie>();
        var query = "MATCH (m:Movie) WHERE m.id STARTS WITH 'stream-movie-' RETURN m AS movie ORDER BY m.id";
        
        await foreach (var movie in Repo.ExecuteReadStreamAsync<Movie>(query, "movie"))
        {
            streamedMovies.Add(movie);
        }

        // Assert
        Assert.That(streamedMovies.Count, Is.EqualTo(100));
        Assert.That(streamedMovies[0].Id, Is.EqualTo("stream-movie-1"));
        Assert.That(streamedMovies[99].Id, Is.EqualTo("stream-movie-99"));
    }

    [Test]
    public async Task ExecuteReadStreamAsync_WithParameters_FiltersCorrectly()
    {
        // Arrange
        var movies = new List<Movie>
        {
            new() { Id = "param-1", Title = "Old Movie", Released = 2010, Tagline = "Old" },
            new() { Id = "param-2", Title = "New Movie", Released = 2023, Tagline = "New" },
            new() { Id = "param-3", Title = "Recent Movie", Released = 2020, Tagline = "Recent" }
        };
        await Repo.UpsertNodes(movies);

        // Act - Stream only movies from 2020 onwards
        var streamedMovies = new List<Movie>();
        var query = "MATCH (m:Movie) WHERE m.id STARTS WITH 'param-' AND m.released >= $year RETURN m AS movie ORDER BY m.released";
        var parameters = new Dictionary<string, object> { { "year", 2020 } };
        
        await foreach (var movie in Repo.ExecuteReadStreamAsync<Movie>(query, "movie", parameters))
        {
            streamedMovies.Add(movie);
        }

        // Assert
        Assert.That(streamedMovies.Count, Is.EqualTo(2));
        Assert.That(streamedMovies.Any(m => m.Id == "param-2"), Is.True);
        Assert.That(streamedMovies.Any(m => m.Id == "param-3"), Is.True);
        Assert.That(streamedMovies.Any(m => m.Id == "param-1"), Is.False);
    }

    [Test]
    public async Task ExecuteReadStreamAsync_WithEmptyResult_ReturnsEmpty()
    {
        // Act - Query for non-existent data
        var streamedMovies = new List<Movie>();
        var query = "MATCH (m:Movie) WHERE m.id = 'non-existent' RETURN m AS movie";
        
        await foreach (var movie in Repo.ExecuteReadStreamAsync<Movie>(query, "movie"))
        {
            streamedMovies.Add(movie);
        }

        // Assert
        Assert.That(streamedMovies, Is.Empty);
    }

    [Test]
    public async Task ExecuteReadStreamAsync_EarlyBreak_DisposesCorrectly()
    {
        // Arrange
        var movies = Enumerable.Range(1, 50)
            .Select(i => new Movie 
            { 
                Id = $"break-movie-{i}", 
                Title = $"Movie {i}", 
                Released = 2020, 
                Tagline = "Test" 
            })
            .ToList();
        await Repo.UpsertNodes(movies);

        // Act - Take only first 10, then break
        var streamedMovies = new List<Movie>();
        var query = "MATCH (m:Movie) WHERE m.id STARTS WITH 'break-movie-' RETURN m AS movie ORDER BY m.id";
        
        await foreach (var movie in Repo.ExecuteReadStreamAsync<Movie>(query, "movie"))
        {
            streamedMovies.Add(movie);
            if (streamedMovies.Count >= 10)
                break;
        }

        // Assert
        Assert.That(streamedMovies.Count, Is.EqualTo(10));
    }

    #endregion

    #region ExecuteReadListStringsAsync Tests

    [Test]
    public async Task ExecuteReadListStringsAsync_ReturnsStringList()
    {
        // Arrange - Create movies with specific genres
        var movie1 = new Movie { Id = "strings-1", Title = "Action Movie", Released = 2021, Tagline = "T1" };
        var movie2 = new Movie { Id = "strings-2", Title = "Comedy Movie", Released = 2022, Tagline = "T2" };
        var actionGenre = new Genre { Id = "action", Name = "Action", Description = "Action" };
        var comedyGenre = new Genre { Id = "comedy", Name = "Comedy", Description = "Comedy" };

        await Repo.UpsertNodes(new[] { movie1, movie2 });
        await Repo.UpsertNodes(new[] { actionGenre, comedyGenre });
        await Repo.MergeRelationshipAsync(movie1, "IN_GENRE", actionGenre);
        await Repo.MergeRelationshipAsync(movie2, "IN_GENRE", comedyGenre);

        // Act - Get list of genre names
        var query = """
            MATCH (m:Movie)-[:IN_GENRE]->(g:Genre)
            WHERE m.id IN ['strings-1', 'strings-2']
            RETURN collect(DISTINCT g.name) AS genres
            """;
        var genreNames = await Repo.ExecuteReadListStringsAsync(query, "genres");

        // Assert
        var genreList = genreNames.ToList();
        Assert.That(genreList.Count, Is.EqualTo(2));
        Assert.That(genreList, Does.Contain("Action"));
        Assert.That(genreList, Does.Contain("Comedy"));
    }

    [Test]
    public async Task ExecuteReadListStringsAsync_WithParameters_FiltersCorrectly()
    {
        // Arrange
        var movies = new List<Movie>
        {
            new() { Id = "str-param-1", Title = "Movie A", Released = 2020, Tagline = "A" },
            new() { Id = "str-param-2", Title = "Movie B", Released = 2021, Tagline = "B" },
            new() { Id = "str-param-3", Title = "Movie C", Released = 2022, Tagline = "C" }
        };
        await Repo.UpsertNodes(movies);

        // Act - Get titles for movies after specific year
        var query = """
            MATCH (m:Movie)
            WHERE m.id STARTS WITH 'str-param-' AND m.released >= $year
            RETURN collect(m.title) AS titles
            """;
        var parameters = new Dictionary<string, object> { { "year", 2021 } };
        var titles = await Repo.ExecuteReadListStringsAsync(query, "titles", parameters);

        // Assert
        var titleList = titles.ToList();
        Assert.That(titleList.Count, Is.EqualTo(2));
        Assert.That(titleList, Does.Contain("Movie B"));
        Assert.That(titleList, Does.Contain("Movie C"));
    }

    [Test]
    public async Task ExecuteReadListStringsAsync_WithEmptyResult_ReturnsEmpty()
    {
        // Act - Query for non-existent data
        var query = """
            MATCH (m:Movie)
            WHERE m.id = 'non-existent'
            RETURN collect(m.title) AS titles
            """;
        var titles = await Repo.ExecuteReadListStringsAsync(query, "titles");

        // Assert
        Assert.That(titles, Is.Empty);
    }

    [Test]
    public async Task ExecuteReadListStringsAsync_WithDuplicates_ReturnsDistinct()
    {
        // Arrange - Create movies with duplicate genres
        var movie1 = new Movie { Id = "dup-1", Title = "Action 1", Released = 2021, Tagline = "T1" };
        var movie2 = new Movie { Id = "dup-2", Title = "Action 2", Released = 2022, Tagline = "T2" };
        var actionGenre = new Genre { Id = "dup-action", Name = "Action", Description = "Action" };

        await Repo.UpsertNodes(new[] { movie1, movie2 });
        await Repo.UpsertNode(actionGenre);
        await Repo.MergeRelationshipAsync(movie1, "IN_GENRE", actionGenre);
        await Repo.MergeRelationshipAsync(movie2, "IN_GENRE", actionGenre);

        // Act - Get distinct genre names
        var query = """
            MATCH (m:Movie)-[:IN_GENRE]->(g:Genre)
            WHERE m.id IN ['dup-1', 'dup-2']
            RETURN collect(g.name) AS genres
            """;
        var genreNames = await Repo.ExecuteReadListStringsAsync(query, "genres");

        // Assert - Should be deduplicated
        var genreList = genreNames.ToList();
        Assert.That(genreList.Count, Is.EqualTo(1));
        Assert.That(genreList[0], Is.EqualTo("Action"));
    }

    #endregion

    #region ExecuteReadListAsync Edge Cases

    [Test]
    public async Task ExecuteReadListAsync_WithInvalidAlias_ThrowsRepositoryException()
    {
        // Arrange
        await Repo.UpsertNode(new Movie { Id = "alias-test", Title = "Test", Released = 2020, Tagline = "T" });

        // Act & Assert - Wrong alias should throw RepositoryException wrapping KeyNotFoundException
        var query = "MATCH (m:Movie) WHERE m.id = 'alias-test' RETURN m AS wrongAlias";
        var ex = Assert.ThrowsAsync<RepositoryException>(async () =>
            await Repo.ExecuteReadListAsync<Movie>(query, "expectedAlias"));
        Assert.That(ex!.InnerException, Is.InstanceOf<KeyNotFoundException>());
    }

    [Test]
    public async Task ExecuteReadListAsync_WithSession_ReusesSession()
    {
        // Arrange
        var movies = new List<Movie>
        {
            new() { Id = "session-1", Title = "Movie 1", Released = 2021, Tagline = "T1" },
            new() { Id = "session-2", Title = "Movie 2", Released = 2022, Tagline = "T2" }
        };
        await Repo.UpsertNodes(movies);

        // Act - Multiple queries with same session
        await using var session = Repo.StartSession();
        
        var query1 = "MATCH (m:Movie) WHERE m.id = 'session-1' RETURN m AS movie";
        var query2 = "MATCH (m:Movie) WHERE m.id = 'session-2' RETURN m AS movie";
        
        var result1 = await Repo.ExecuteReadListAsync<Movie>(query1, "movie", session);
        var result2 = await Repo.ExecuteReadListAsync<Movie>(query2, "movie", session);

        // Assert
        Assert.That(result1.Count(), Is.EqualTo(1));
        Assert.That(result2.Count(), Is.EqualTo(1));
        Assert.That(result1.First().Id, Is.EqualTo("session-1"));
        Assert.That(result2.First().Id, Is.EqualTo("session-2"));
    }

    [Test]
    public async Task ExecuteReadListAsync_WithComplexCypher_ReturnsCorrectResults()
    {
        // Arrange - Create complex graph structure
        var movie = new Movie { Id = "complex-movie", Title = "Complex Movie", Released = 2023, Tagline = "Complex" };
        var genre1 = new Genre { Id = "complex-g1", Name = "Action", Description = "A" };
        var genre2 = new Genre { Id = "complex-g2", Name = "Drama", Description = "D" };

        await Repo.UpsertNode(movie);
        await Repo.UpsertNodes(new[] { genre1, genre2 });
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre1);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre2);

        // Act - Complex query with multiple matches
        var query = """
            MATCH (m:Movie {id: $movieId})-[:IN_GENRE]->(g:Genre)
            RETURN g AS genre
            ORDER BY g.name
            """;
        var parameters = new Dictionary<string, object> { { "movieId", "complex-movie" } };
        var genres = await Repo.ExecuteReadListAsync<Genre>(query, "genre", parameters);

        // Assert
        var genreList = genres.ToList();
        Assert.That(genreList.Count, Is.EqualTo(2));
        Assert.That(genreList[0].Name, Is.EqualTo("Action"));
        Assert.That(genreList[1].Name, Is.EqualTo("Drama"));
    }

    #endregion

    #region ExecuteReadScalarAsync Tests

    [Test]
    public async Task ExecuteReadScalarAsync_ReturnsCount()
    {
        // Arrange
        var movies = Enumerable.Range(1, 15)
            .Select(i => new Movie 
            { 
                Id = $"scalar-{i}", 
                Title = $"Movie {i}", 
                Released = 2020, 
                Tagline = "T" 
            })
            .ToList();
        await Repo.UpsertNodes(movies);

        // Act
        var count = await Repo.ExecuteReadScalarAsync<long>(
            "MATCH (m:Movie) WHERE m.id STARTS WITH 'scalar-' RETURN count(m)");

        // Assert
        Assert.That(count, Is.EqualTo(15));
    }

    [Test]
    public async Task ExecuteReadScalarAsync_WithParameters_ReturnsFilteredResult()
    {
        // Arrange
        var movies = new List<Movie>
        {
            new() { Id = "scalar-param-1", Title = "Old", Released = 2010, Tagline = "T" },
            new() { Id = "scalar-param-2", Title = "New", Released = 2023, Tagline = "T" },
            new() { Id = "scalar-param-3", Title = "Recent", Released = 2020, Tagline = "T" }
        };
        await Repo.UpsertNodes(movies);

        // Act
        var count = await Repo.ExecuteReadScalarAsync<long>(
            "MATCH (m:Movie) WHERE m.id STARTS WITH 'scalar-param-' AND m.released >= $year RETURN count(m)",
            new Dictionary<string, object> { { "year", 2020 } });

        // Assert
        Assert.That(count, Is.EqualTo(2));
    }

    [Test]
    public async Task ExecuteReadScalarAsync_ReturnsMaxValue()
    {
        // Arrange
        var movies = new List<Movie>
        {
            new() { Id = "max-1", Title = "M1", Released = 2010, Tagline = "T" },
            new() { Id = "max-2", Title = "M2", Released = 2023, Tagline = "T" },
            new() { Id = "max-3", Title = "M3", Released = 2020, Tagline = "T" }
        };
        await Repo.UpsertNodes(movies);

        // Act
        var maxYear = await Repo.ExecuteReadScalarAsync<long>(
            "MATCH (m:Movie) WHERE m.id STARTS WITH 'max-' RETURN max(m.released)");

        // Assert
        Assert.That(maxYear, Is.EqualTo(2023));
    }

    [Test]
    public async Task ExecuteReadScalarAsync_ReturnsStringValue()
    {
        // Arrange
        await Repo.UpsertNode(new Movie 
        { 
            Id = "str-scalar", 
            Title = "Specific Title", 
            Released = 2020, 
            Tagline = "T" 
        });

        // Act
        var title = await Repo.ExecuteReadScalarAsync<string>(
            "MATCH (m:Movie {id: 'str-scalar'}) RETURN m.title");

        // Assert
        Assert.That(title, Is.EqualTo("Specific Title"));
    }

    #endregion
}
