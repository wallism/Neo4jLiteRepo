using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.NodeServices;
using Neo4jLiteRepo.Sample.Nodes;
using Neo4jLiteRepo.Sample.NodeServices;
using NSubstitute;
using NUnit.Framework;
using Serilog;

namespace Neo4jLiteRepo.Tests
{
    [TestFixture]
    public class DataSourceServiceTests
    {
        private ILogger<DataSourceService> _logger;
        private IServiceProvider _serviceProvider;
        private DataSourceService _dataSourceService;
        private IConfiguration _configuration;

        [OneTimeSetUp]
        public void FixtureSetUp()
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.Debug()
                .CreateLogger();
            // Convert to Microsoft.Extensions.Logging.ILogger<DataSourceService>
            var loggerFactory = new LoggerFactory().AddSerilog(Log.Logger);
            _logger = loggerFactory.CreateLogger<DataSourceService>();

            _configuration = Substitute.For<IConfiguration>();

            // Setup the configuration section mock
            _configuration["Neo4jLiteRepo:JsonFilePath"]
                .Returns(".\\Nodes\\Data\\");
        }

        [OneTimeTearDown]
        public void GlobalTestTeardown() => Log.CloseAndFlush();

        [SetUp]
        public void SetUp()
        {
            // create a real ServiceProvider because alot of logic depends on the service collection.
            var services = new ServiceCollection();
            services.AddSingleton<IConfiguration>(_ => _configuration);
            services.AddSingleton<IDataRefreshPolicy, DataRefreshPolicy>();

            var drpLogger = Substitute.For<ILogger<DataRefreshPolicy>>();
            services.AddSingleton(drpLogger);

            // Register DataSourceService and its interface before node services that depend on it
            services.AddSingleton<ILogger<DataSourceService>>(_logger);
            services.AddSingleton<DataSourceService>();
            services.AddSingleton<IDataSourceService>(sp => sp.GetRequiredService<DataSourceService>());

            services.AddSingleton<INodeService, GenreNodeService>();
            services.AddSingleton<INodeService, MovieNodeService>();

            _serviceProvider = services.BuildServiceProvider();
            _dataSourceService = _serviceProvider.GetRequiredService<DataSourceService>();
        }

        [TearDown]
        public void Teardown()
        {
            if (_serviceProvider is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }
        
        [Test]
        public async Task GetAllSourceNodes_ReturnsAllNodes()
        {
            // Arrange
            await _dataSourceService.LoadAllNodeDataAsync();

            // Act
            var result = _dataSourceService.GetAllSourceNodes();

            // Assert
            Assert.That(result.Count, Is.EqualTo(2), "Movie and Genre");
            Assert.That(result.ContainsKey("Movie"));
            Assert.That(result.ContainsKey("Genre"));
        }

        [Test]
        public async Task GetSourceNodesFor_Movie_ReturnsNodes()
        {
            // Arrange
            await _dataSourceService.LoadAllNodeDataAsync();

            // Act
            var result = _dataSourceService.GetSourceNodesFor<Movie>();

            // Assert 
            Assert.That(result.Count(), Is.EqualTo(3));
        }

        [Test]
        public async Task GetSourceNodesFor_Type_ReturnsNodes()
        {
            // Arrange
            await _dataSourceService.LoadAllNodeDataAsync();

            // Act
            var result = _dataSourceService.GetSourceNodesFor<Genre>();

            // Assert
            Assert.That(result.Count(), Is.EqualTo(4));
        }


        [Test]
        public async Task GetSourceNodeFor_FindingNemo_ReturnsNode()
        {
            // Arrange
            await _dataSourceService.LoadAllNodeDataAsync();

            // Act
            var result = _dataSourceService.GetSourceNodeFor<Movie>("Movie", "2");

            // Assert
            Assert.That(result, Is.Not.Null);
            Assert.That(result.GetPrimaryKeyValue(), Is.EqualTo("2"));
            Assert.That(result.Title, Is.EqualTo("Finding Nemo"));
        }

        [Test]
        public async Task GetSourceNodeByDisplayName_FindingNemo_ReturnsNode()
        {
            // Arrange
            await _dataSourceService.LoadAllNodeDataAsync();

            // Act
            var result = _dataSourceService.GetSourceNodeByDisplayName<Movie>("Finding Nemo");

            // Assert
            Assert.That(result, Is.Not.Null);
            Assert.That(result.DisplayName, Is.EqualTo("Finding Nemo"));
            Assert.That(result.GetPrimaryKeyValue(), Is.EqualTo("2"));
        }

    }
}
