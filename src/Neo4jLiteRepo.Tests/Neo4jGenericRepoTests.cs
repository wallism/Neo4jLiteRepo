using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Sample.Nodes;
using NSubstitute;
using NUnit.Framework;
using Serilog;

namespace Neo4jLiteRepo.Tests;

/// <summary>
/// Unit tests for Neo4jGenericRepo using NSubstitute mocks.
/// These tests verify method behavior and Cypher query generation without requiring a live Neo4j instance.
/// </summary>
[TestFixture]
public class Neo4jGenericRepoTests
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

        // Setup default mock behavior
        _mockDriver.AsyncSession().Returns(_mockSession);
        _mockSession.BeginTransactionAsync().Returns(_mockTransaction);
        _mockCursor.ConsumeAsync().Returns(_mockSummary);

        _repo = new Neo4jGenericRepo(_logger, _configuration, _mockDriver, _mockDataSourceService);
    }

    [TearDown]
    public async Task TearDown()
    {
        _repo.Dispose();
        // Dispose mocks that implement IAsyncDisposable/IDisposable
        await _mockSession.DisposeAsync();
        await _mockTransaction.DisposeAsync();
        await _mockDriver.DisposeAsync();
    }

    #region StartSession Tests

    [Test]
    public void StartSession_ReturnsAsyncSessionFromDriver()
    {
        // Act
        var session = _repo.StartSession();

        // Assert
        Assert.That(session, Is.EqualTo(_mockSession));
        _mockDriver.Received(1).AsyncSession();
    }

    #endregion

    #region UpsertNode Tests

    [Test]
    public async Task UpsertNode_WithCancellationToken_CreatesSessionAndTransaction()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test Movie", Released = 2023, Tagline = "A test movie" };
        SetupTransactionRunAsync();

        // Act
        await _repo.UpsertNode(movie);

        // Assert
        _mockDriver.Received(1).AsyncSession();
        await _mockSession.Received(1).BeginTransactionAsync();
        await _mockTransaction.Received(1).CommitAsync();
    }

    [Test]
    public async Task UpsertNode_WithExistingSession_UsesProvidedSession()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test Movie", Released = 2023, Tagline = "A test movie" };
        SetupTransactionRunAsync();

        // Act
        await _repo.UpsertNode(movie, _mockSession);

        // Assert
        _mockDriver.DidNotReceive().AsyncSession();
        await _mockSession.Received(1).BeginTransactionAsync();
        await _mockTransaction.Received(1).CommitAsync();
    }

    [Test]
    public async Task UpsertNode_WithExistingTransaction_UsesProvidedTransaction()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test Movie", Released = 2023, Tagline = "A test movie" };
        SetupTransactionRunAsync();

        // Act
        await _repo.UpsertNode(movie, _mockTransaction);

        // Assert
        _mockDriver.DidNotReceive().AsyncSession();
        await _mockSession.DidNotReceive().BeginTransactionAsync();
        await _mockTransaction.DidNotReceive().CommitAsync(); // Transaction not committed when provided externally
    }

    [Test]
    public async Task UpsertNode_ExecutesMergeQuery()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test Movie", Released = 2023, Tagline = "A test movie" };
        string? capturedQuery = null;

        _mockTransaction.RunAsync(Arg.Do<string>(q => capturedQuery = q), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        await _repo.UpsertNode(movie, _mockTransaction);

        // Assert
        Assert.That(capturedQuery, Is.Not.Null);
        Assert.That(capturedQuery, Does.Contain("MERGE"));
        Assert.That(capturedQuery, Does.Contain("Movie"));
    }

    [Test]
    public async Task UpsertNode_OnException_RollsBackTransaction()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test Movie", Released = 2023, Tagline = "A test movie" };
        _mockTransaction.RunAsync(Arg.Any<string>(), Arg.Any<object>())
            .Returns<IResultCursor>(_ => throw new Exception("Test exception"));

        // Act & Assert
        Assert.ThrowsAsync<Exception>(async () => await _repo.UpsertNode(movie, _mockSession));
        await _mockTransaction.Received(1).RollbackAsync();
    }

    [Test]
    public void UpsertNode_WhenCancelled_ThrowsOperationCanceledException()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test Movie", Released = 2023, Tagline = "A test movie" };
        var cts = new CancellationTokenSource();
        cts.Cancel();
        SetupTransactionRunAsync();

        // Act & Assert
        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await _repo.UpsertNode(movie, _mockTransaction, cts.Token));
    }

    #endregion

    #region UpsertNodes Tests

    [Test]
    public async Task UpsertNodes_UpsertsAllNodesInCollection()
    {
        // Arrange
        var movies = new List<Movie>
        {
            new() { Id = "1", Title = "Movie 1", Released = 2021, Tagline = "Tag 1" },
            new() { Id = "2", Title = "Movie 2", Released = 2022, Tagline = "Tag 2" },
            new() { Id = "3", Title = "Movie 3", Released = 2023, Tagline = "Tag 3" }
        };
        SetupTransactionRunAsync();

        // Act
        var results = await _repo.UpsertNodes(movies);

        // Assert
        Assert.That(results.Count(), Is.EqualTo(3));
        await _mockTransaction.Received(3).RunAsync(Arg.Any<string>(), Arg.Any<object>());
    }

    [Test]
    public async Task UpsertNodes_WithExistingTransaction_DoesNotCommit()
    {
        // Arrange
        var movies = new List<Movie>
        {
            new() { Id = "1", Title = "Movie 1", Released = 2021, Tagline = "Tag 1" }
        };
        SetupTransactionRunAsync();

        // Act
        await _repo.UpsertNodes(movies, _mockTransaction);

        // Assert
        await _mockTransaction.DidNotReceive().CommitAsync();
    }

    #endregion

    #region DetachDeleteAsync Tests

    [Test]
    public async Task DetachDeleteAsync_ByNode_ExecutesDetachDeleteQuery()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test Movie", Released = 2023, Tagline = "A test movie" };
        string? capturedQuery = null;

        _mockTransaction.RunAsync(Arg.Do<string>(q => capturedQuery = q), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        await _repo.DetachDeleteAsync(movie, _mockTransaction);

        // Assert
        Assert.That(capturedQuery, Is.Not.Null);
        Assert.That(capturedQuery, Does.Contain("DETACH DELETE"));
        Assert.That(capturedQuery, Does.Contain("Movie"));
    }

    [Test]
    public async Task DetachDeleteAsync_ByPrimaryKey_ExecutesDetachDeleteQuery()
    {
        // Arrange
        string? capturedQuery = null;

        _mockTransaction.RunAsync(Arg.Do<string>(q => capturedQuery = q), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        await _repo.DetachDeleteAsync<Movie>("1", _mockTransaction);

        // Assert
        Assert.That(capturedQuery, Is.Not.Null);
        Assert.That(capturedQuery, Does.Contain("DETACH DELETE"));
        Assert.That(capturedQuery, Does.Contain("Movie"));
    }

    [Test]
    public void DetachDeleteAsync_WithNullNode_ThrowsArgumentNullException()
    {
        // Arrange
        Movie? nullMovie = null;
        
        // Act & Assert
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await _repo.DetachDeleteAsync(nullMovie!, _mockTransaction));
    }

    [Test]
    public void DetachDeleteAsync_WithNullTransaction_ThrowsArgumentNullException()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test Movie", Released = 2023, Tagline = "A test movie" };

        // Act & Assert
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await _repo.DetachDeleteAsync(movie, (IAsyncTransaction)null!));
    }

    #endregion

    #region DetachDeleteManyAsync Tests

    [Test]
    public async Task DetachDeleteManyAsync_ByNodes_ExecutesBatchedDelete()
    {
        // Arrange
        var movies = new List<Movie>
        {
            new() { Id = "1", Title = "Movie 1", Released = 2021, Tagline = "Tag 1" },
            new() { Id = "2", Title = "Movie 2", Released = 2022, Tagline = "Tag 2" }
        };
        SetupTransactionRunAsync();

        // Act
        await _repo.DetachDeleteManyAsync(movies, _mockTransaction);

        // Assert
        // Should execute a batched query (uses WHERE IN for small batches, UNWIND for large)
        await _mockTransaction.Received().RunAsync(
            Arg.Is<string>(s => s.Contains("DETACH DELETE") && s.Contains("Movie")), 
            Arg.Any<object>());
    }

    [Test]
    public async Task DetachDeleteManyAsync_ByIds_ExecutesBatchedDelete()
    {
        // Arrange
        var ids = new List<string> { "1", "2", "3" };
        SetupTransactionRunAsync();

        // Act
        await _repo.DetachDeleteManyAsync<Movie>(ids, _mockTransaction);

        // Assert
        // Should execute a batched query
        await _mockTransaction.Received().RunAsync(
            Arg.Is<string>(s => s.Contains("DETACH DELETE") && s.Contains("Movie")), 
            Arg.Any<object>());
    }

    [Test]
    public void DetachDeleteManyAsync_WithEmptyList_ThrowsArgumentNullException()
    {
        // Arrange - empty list throws ArgumentNullException (misnamed param check in implementation)
        var ids = new List<string>();

        // Act & Assert
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await _repo.DetachDeleteManyAsync<Movie>(ids, _mockTransaction));
    }

    #endregion

    #region MergeRelationshipAsync Tests

    [Test]
    public async Task MergeRelationshipAsync_CreatesMergeQuery()
    {
        // Arrange
        var fromMovie = new Movie { Id = "1", Title = "Movie 1", Released = 2021, Tagline = "Tag 1" };
        var toGenre = new Genre { Id = "g1", Name = "Action" };
        string? capturedQuery = null;

        _mockTransaction.RunAsync(Arg.Do<string>(q => capturedQuery = q), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        await _repo.MergeRelationshipAsync(fromMovie, "IN_GENRE", toGenre, _mockTransaction);

        // Assert
        Assert.That(capturedQuery, Is.Not.Null);
        Assert.That(capturedQuery, Does.Contain("MERGE"));
        Assert.That(capturedQuery, Does.Contain("IN_GENRE"));
    }

    [Test]
    public void MergeRelationshipAsync_WithNullTransaction_ThrowsArgumentNullException()
    {
        // Arrange
        var fromMovie = new Movie { Id = "1", Title = "Movie 1", Released = 2021, Tagline = "Tag 1" };
        var toGenre = new Genre { Id = "g1", Name = "Action" };

        // Act & Assert
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await _repo.MergeRelationshipAsync(fromMovie, "IN_GENRE", toGenre, (IAsyncTransaction)null!));
    }

    [Test]
    public void MergeRelationshipAsync_WithNullRelType_ThrowsArgumentException()
    {
        // Arrange
        var fromMovie = new Movie { Id = "1", Title = "Movie 1", Released = 2021, Tagline = "Tag 1" };
        var toGenre = new Genre { Id = "g1", Name = "Action" };

        // Act & Assert
        Assert.ThrowsAsync<ArgumentException>(
            async () => await _repo.MergeRelationshipAsync(fromMovie, null!, toGenre, _mockTransaction));
    }

    #endregion

    #region DeleteRelationshipAsync Tests

    [Test]
    public async Task DeleteRelationshipAsync_CreatesDeleteQuery()
    {
        // Arrange
        var fromMovie = new Movie { Id = "1", Title = "Movie 1", Released = 2021, Tagline = "Tag 1" };
        var toGenre = new Genre { Id = "g1", Name = "Action" };
        string? capturedQuery = null;

        _mockTransaction.RunAsync(Arg.Do<string>(q => capturedQuery = q), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        await _repo.DeleteRelationshipAsync(fromMovie, "IN_GENRE", toGenre, 
            Neo4jLiteRepo.Models.EdgeDirection.Outgoing, _mockTransaction);

        // Assert
        Assert.That(capturedQuery, Is.Not.Null);
        Assert.That(capturedQuery, Does.Contain("DELETE"));
        Assert.That(capturedQuery, Does.Contain("IN_GENRE"));
    }

    #endregion

    #region ExecuteReadListAsync Tests

    [Test]
    public async Task ExecuteReadListAsync_UsesSessionExecuteReadAsync()
    {
        // Arrange - Mock ExecuteReadAsync to return an empty list
        _mockSession.ExecuteReadAsync(Arg.Any<Func<IAsyncQueryRunner, Task<List<Movie>>>>())
            .Returns(new List<Movie>());

        // Act
        var results = await _repo.ExecuteReadListAsync<Movie>("MATCH (m:Movie) RETURN m", "m");

        // Assert
        await _mockSession.Received(1).ExecuteReadAsync(Arg.Any<Func<IAsyncQueryRunner, Task<List<Movie>>>>());
    }

    [Test]
    public async Task ExecuteReadListAsync_WithSession_UsesProvidedSession()
    {
        // Arrange
        _mockSession.ExecuteReadAsync(Arg.Any<Func<IAsyncQueryRunner, Task<List<Movie>>>>())
            .Returns(new List<Movie>());

        // Act
        var results = await _repo.ExecuteReadListAsync<Movie>("MATCH (m:Movie) RETURN m", "m", _mockSession);

        // Assert
        _mockDriver.DidNotReceive().AsyncSession(); // Should not create new session
        await _mockSession.Received(1).ExecuteReadAsync(Arg.Any<Func<IAsyncQueryRunner, Task<List<Movie>>>>());
    }

    #endregion

    #region ExecuteReadScalarAsync Tests

    [Test]
    public async Task ExecuteReadScalarAsync_UsesSessionExecuteReadAsync()
    {
        // Arrange - Mock ExecuteReadAsync to return a scalar value
        _mockSession.ExecuteReadAsync(Arg.Any<Func<IAsyncQueryRunner, Task<long>>>())
            .Returns(42L);

        // Act
        var result = await _repo.ExecuteReadScalarAsync<long>("MATCH (m:Movie) RETURN count(m)");

        // Assert
        await _mockSession.Received(1).ExecuteReadAsync(Arg.Any<Func<IAsyncQueryRunner, Task<long>>>());
    }

    #endregion

    #region Helper Methods

    private void SetupTransactionRunAsync()
    {
        _mockTransaction.RunAsync(Arg.Any<string>(), Arg.Any<object>())
            .Returns(_mockCursor);
    }

    #endregion
}
