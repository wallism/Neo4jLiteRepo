using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Sample.Nodes;
using NSubstitute;
using NUnit.Framework;
using Serilog;

namespace Neo4jLiteRepo.UnitTests;

/// <summary>
/// Unit tests for Neo4jGenericRepo maintenance operations - RemoveOrphansAsync.
/// </summary>
[TestFixture]
public class MaintenanceTests
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

    #region RemoveOrphansAsync Tests

    [Test]
    public async Task RemoveOrphansAsync_WithTransaction_ExecutesBatchedDeleteQuery()
    {
        // Arrange
        var mockRecord = Substitute.For<IRecord>();
        mockRecord["deleted"].Returns((object)5);

        // First call returns 5 (less than batch size 400), signaling completion
        _mockCursor.FetchAsync().Returns(true, false);
        _mockCursor.Current.Returns(mockRecord);
        _mockTransaction.RunAsync(Arg.Any<string>(), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        var result = await _repo.RemoveOrphansAsync<Movie>(_mockTransaction);

        // Assert
        Assert.That(result, Is.EqualTo(5));
        await _mockTransaction.Received().RunAsync(
            Arg.Is<string>(s => s.Contains("DETACH DELETE") && s.Contains("Movie") && s.Contains("NOT (n)--()") ),
            Arg.Any<object>());
    }

    [Test]
    public void RemoveOrphansAsync_WithNullTransaction_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await _repo.RemoveOrphansAsync<Movie>((IAsyncTransaction)null!));
    }

    [Test]
    public void RemoveOrphansAsync_WithNullSession_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await _repo.RemoveOrphansAsync<Movie>((IAsyncSession)null!));
    }

    [Test]
    public async Task RemoveOrphansAsync_NoOrphans_ReturnsZero()
    {
        // Arrange - FetchAsync returns true but deleted = 0
        var mockRecord = Substitute.For<IRecord>();
        mockRecord["deleted"].Returns((object)0);

        _mockCursor.FetchAsync().Returns(true, false);
        _mockCursor.Current.Returns(mockRecord);
        _mockTransaction.RunAsync(Arg.Any<string>(), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        var result = await _repo.RemoveOrphansAsync<Movie>(_mockTransaction);

        // Assert
        Assert.That(result, Is.EqualTo(0));
    }

    [Test]
    public async Task RemoveOrphansAsync_WithSession_CommitsTransaction()
    {
        // Arrange
        var mockRecord = Substitute.For<IRecord>();
        mockRecord["deleted"].Returns((object)3);

        _mockCursor.FetchAsync().Returns(true, false);
        _mockCursor.Current.Returns(mockRecord);
        _mockTransaction.RunAsync(Arg.Any<string>(), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        var result = await _repo.RemoveOrphansAsync<Movie>(_mockSession);

        // Assert
        Assert.That(result, Is.EqualTo(3));
        await _mockTransaction.Received(1).CommitAsync();
    }

    [Test]
    public async Task RemoveOrphansAsync_QueryContainsCorrectLabel()
    {
        // Arrange
        string? capturedQuery = null;
        var mockRecord = Substitute.For<IRecord>();
        mockRecord["deleted"].Returns((object)0);

        _mockCursor.FetchAsync().Returns(true, false);
        _mockCursor.Current.Returns(mockRecord);
        _mockTransaction.RunAsync(Arg.Do<string>(q => capturedQuery = q), Arg.Any<object>())
            .Returns(_mockCursor);

        // Act
        await _repo.RemoveOrphansAsync<Genre>(_mockTransaction);

        // Assert
        Assert.That(capturedQuery, Does.Contain("Genre"));
        Assert.That(capturedQuery, Does.Contain("NOT (n)--()"));
        Assert.That(capturedQuery, Does.Contain("DETACH DELETE"));
    }

    #endregion
}
