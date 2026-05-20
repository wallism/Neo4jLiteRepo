using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Models;
using Neo4jLiteRepo.Sample.Nodes;
using NSubstitute;
using NUnit.Framework;
using Serilog;

namespace Neo4jLiteRepo.UnitTests;

/// <summary>
/// Unit tests for Neo4jGenericRepo relationship operations - DeleteEdges, DeleteRelationshipsOfTypeFrom, UpsertRelationships.
/// </summary>
[TestFixture]
public class RelationshipTests
{
    private ILogger<Neo4jGenericRepo> _logger = null!;
    private IConfiguration _configuration = null!;
    private IDriver _mockDriver = null!;
    private IAsyncSession _mockSession = null!;
    private IAsyncTransaction _mockTransaction = null!;
    private IResultCursor _mockCursor = null!;
    private IResultSummary _mockSummary = null!;
    private IDataSourceService _mockDataSourceService = null!;
    private Neo4jGenericRepo _repo = null!;

    [OneTimeSetUp]
    public void FixtureSetUp()
    {
        Log.Logger = new LoggerConfiguration()
            .WriteTo.Console()
            .WriteTo.Debug()
            .CreateLogger();

        var loggerFactory = new LoggerFactory().AddSerilog(Log.Logger);
        _logger = loggerFactory.CreateLogger<Neo4jGenericRepo>();
    }

    [OneTimeTearDown]
    public void GlobalTestTeardown() => Log.CloseAndFlush();

    [SetUp]
    public void SetUp()
    {
        _configuration = Substitute.For<IConfiguration>();
        _mockDriver = Substitute.For<IDriver>();
        _mockSession = Substitute.For<IAsyncSession>();
        _mockTransaction = Substitute.For<IAsyncTransaction>();
        _mockCursor = Substitute.For<IResultCursor>();
        _mockSummary = Substitute.For<IResultSummary>();
        _mockDataSourceService = Substitute.For<IDataSourceService>();

        _mockDriver.AsyncSession().Returns(_mockSession);
        _mockSession.BeginTransactionAsync().Returns(_mockTransaction);
        _mockSession.BeginTransactionAsync(Arg.Any<Action<TransactionConfigBuilder>>())
            .Returns(_mockTransaction);
        _mockCursor.ConsumeAsync().Returns(_mockSummary);

        _repo = new Neo4jGenericRepo(_logger, _configuration, _mockDriver, _mockDataSourceService);
    }

    [TearDown]
    public async Task TearDown()
    {
        _repo.Dispose();
        await _mockSession.DisposeAsync();
        await _mockTransaction.DisposeAsync();
        await _mockDriver.DisposeAsync();
    }

    #region DeleteEdgesAsync Tests

    [Test]
    public void DeleteEdgesAsync_WithNullTransaction_ThrowsArgumentNullException()
    {
        // Arrange
        var specs = new List<EdgeDeleteSpec>
        {
            new(new Movie { Id = "1", Title = "M1" }, "IN_GENRE", new Genre { Id = "g1", Name = "Action" }, EdgeDirection.Outgoing)
        };

        // Act & Assert
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await _repo.DeleteEdgesAsync(specs, null!));
    }

    [Test]
    public void DeleteEdgesAsync_WithNullSpecs_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await _repo.DeleteEdgesAsync(null!, _mockTransaction));
    }

    [Test]
    public async Task DeleteEdgesAsync_EmptySpecs_DoesNothing()
    {
        // Arrange
        var specs = new List<EdgeDeleteSpec>();

        // Act
        await _repo.DeleteEdgesAsync(specs, _mockTransaction);

        // Assert - no query should be executed
        await _mockTransaction.DidNotReceive().RunAsync(Arg.Any<string>(), Arg.Any<object>());
    }

    [Test]
    public async Task DeleteEdgesAsync_ValidSpecs_ExecutesUnwindDeleteQuery()
    {
        // Arrange
        string? capturedQuery = null;
        var specs = new List<EdgeDeleteSpec>
        {
            new(new Movie { Id = "1", Title = "M1" }, "IN_GENRE", new Genre { Id = "g1", Name = "Action" }, EdgeDirection.Outgoing),
            new(new Movie { Id = "2", Title = "M2" }, "IN_GENRE", new Genre { Id = "g2", Name = "Comedy" }, EdgeDirection.Outgoing)
        };

        _mockTransaction.RunAsync(Arg.Do<string>(q => capturedQuery = q), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        await _repo.DeleteEdgesAsync(specs, _mockTransaction);

        // Assert
        Assert.That(capturedQuery, Is.Not.Null);
        Assert.That(capturedQuery, Does.Contain("UNWIND"));
        Assert.That(capturedQuery, Does.Contain("DELETE r"));
        Assert.That(capturedQuery, Does.Contain("IN_GENRE"));
    }

    [Test]
    public async Task DeleteEdgesAsync_SpecsWithEmptyRel_FiltersOutInvalidSpecs()
    {
        // Arrange - one spec has empty rel, should be filtered
        var specs = new List<EdgeDeleteSpec>
        {
            new(new Movie { Id = "1", Title = "M1" }, "", new Genre { Id = "g1", Name = "Action" }, EdgeDirection.Outgoing),
            new(new Movie { Id = "1", Title = "M1" }, "   ", new Genre { Id = "g1", Name = "Action" }, EdgeDirection.Outgoing)
        };

        // Act
        await _repo.DeleteEdgesAsync(specs, _mockTransaction);

        // Assert - empty rel specs are filtered, so no query should run
        await _mockTransaction.DidNotReceive().RunAsync(Arg.Any<string>(), Arg.Any<object>());
    }

    #endregion

    #region DeleteRelationshipsOfTypeFromAsync Tests

    [Test]
    public async Task DeleteRelationshipsOfTypeFromAsync_Outgoing_CreatesCorrectQuery()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test Movie" };
        string? capturedQuery = null;

        _mockTransaction.RunAsync(Arg.Do<string>(q => capturedQuery = q), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        await _repo.DeleteRelationshipsOfTypeFromAsync(movie, "IN_GENRE", EdgeDirection.Outgoing, _mockTransaction);

        // Assert
        Assert.That(capturedQuery, Does.Contain("Movie"));
        Assert.That(capturedQuery, Does.Contain("IN_GENRE"));
        Assert.That(capturedQuery, Does.Contain("->()"));
        Assert.That(capturedQuery, Does.Contain("DELETE r"));
    }

    [Test]
    public async Task DeleteRelationshipsOfTypeFromAsync_Incoming_CreatesCorrectPattern()
    {
        // Arrange
        var genre = new Genre { Id = "g1", Name = "Action" };
        string? capturedQuery = null;

        _mockTransaction.RunAsync(Arg.Do<string>(q => capturedQuery = q), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        await _repo.DeleteRelationshipsOfTypeFromAsync(genre, "IN_GENRE", EdgeDirection.Incoming, _mockTransaction);

        // Assert
        Assert.That(capturedQuery, Does.Contain("<-[r:IN_GENRE]-()"));
    }

    [Test]
    public async Task DeleteRelationshipsOfTypeFromAsync_Both_CreatesCorrectPattern()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test" };
        string? capturedQuery = null;

        _mockTransaction.RunAsync(Arg.Do<string>(q => capturedQuery = q), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        await _repo.DeleteRelationshipsOfTypeFromAsync(movie, "RELATED_TO", EdgeDirection.Both, _mockTransaction);

        // Assert
        Assert.That(capturedQuery, Does.Contain("-[r:RELATED_TO]-"));
    }

    [Test]
    public void DeleteRelationshipsOfTypeFromAsync_NullTransaction_ThrowsArgumentNullException()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test" };

        // Act & Assert
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await _repo.DeleteRelationshipsOfTypeFromAsync(movie, "IN_GENRE", EdgeDirection.Outgoing, null!));
    }

    [Test]
    public void DeleteRelationshipsOfTypeFromAsync_EmptyRel_ThrowsArgumentException()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test" };

        // Act & Assert
        Assert.ThrowsAsync<ArgumentException>(
            async () => await _repo.DeleteRelationshipsOfTypeFromAsync(movie, "", EdgeDirection.Outgoing, _mockTransaction));
    }

    #endregion

    #region DeleteRelationshipAsync Direction Tests

    [Test]
    public async Task DeleteRelationshipAsync_IncomingDirection_GeneratesIncomingPattern()
    {
        // Arrange
        var fromMovie = new Movie { Id = "1", Title = "Movie 1" };
        var toGenre = new Genre { Id = "g1", Name = "Action" };
        string? capturedQuery = null;

        _mockTransaction.RunAsync(Arg.Do<string>(q => capturedQuery = q), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        await _repo.DeleteRelationshipAsync(fromMovie, "IN_GENRE", toGenre, EdgeDirection.Incoming, _mockTransaction);

        // Assert
        Assert.That(capturedQuery, Does.Contain("<-[r:IN_GENRE]-"));
    }

    [Test]
    public async Task DeleteRelationshipAsync_BothDirection_GeneratesBidirectionalPattern()
    {
        // Arrange
        var fromMovie = new Movie { Id = "1", Title = "Movie 1" };
        var toGenre = new Genre { Id = "g1", Name = "Action" };
        string? capturedQuery = null;

        _mockTransaction.RunAsync(Arg.Do<string>(q => capturedQuery = q), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        await _repo.DeleteRelationshipAsync(fromMovie, "IN_GENRE", toGenre, EdgeDirection.Both, _mockTransaction);

        // Assert
        Assert.That(capturedQuery, Does.Contain("-[r:IN_GENRE]-"));
        Assert.That(capturedQuery, Does.Not.Contain("->"));
        Assert.That(capturedQuery, Does.Not.Contain("<-"));
    }

    #endregion

    #region MergeRelationshipAsync Validation Tests

    [Test]
    public void MergeRelationshipAsync_EmptyFromPrimaryKey_ThrowsArgumentException()
    {
        // Arrange
        var movie = new Movie { Id = "", Title = "No ID" };
        var genre = new Genre { Id = "g1", Name = "Action" };

        // Act & Assert
        Assert.ThrowsAsync<ArgumentException>(
            async () => await _repo.MergeRelationshipAsync(movie, "IN_GENRE", genre, _mockTransaction));
    }

    [Test]
    public void MergeRelationshipAsync_EmptyToPrimaryKey_ThrowsArgumentException()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test" };
        var genre = new Genre { Id = "", Name = "No ID" };

        // Act & Assert
        Assert.ThrowsAsync<ArgumentException>(
            async () => await _repo.MergeRelationshipAsync(movie, "IN_GENRE", genre, _mockTransaction));
    }

    #endregion

    #region UpsertRelationshipsAsync Tests

    [Test]
    public async Task UpsertRelationshipsAsync_EmptyCollection_ReturnsTrue()
    {
        // Act
        var result = await _repo.UpsertRelationshipsAsync<Movie>([]);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public async Task UpsertRelationshipsAsync_WithRelationships_ExecutesMergeQuery()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test Movie", GenreIds = ["g1", "g2"] };
        _mockSession.ExecuteWriteAsync(
            Arg.Any<Func<IAsyncQueryRunner, Task<IResultSummary>>>(),
            Arg.Any<Action<TransactionConfigBuilder>>())
            .Returns(_mockSummary);

        // Act
        var result = await _repo.UpsertRelationshipsAsync(movie);

        // Assert
        Assert.That(result, Is.True);
    }

    #endregion
}
