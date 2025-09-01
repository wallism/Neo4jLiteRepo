using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.NodeServices;
using Neo4jLiteRepo.Sample.NodeServices;
using Neo4jLiteRepo.Sample.Nodes;
using NUnit.Framework;
using Serilog;

namespace Neo4jLiteRepo.Tests.Integration
{
    /// <summary>
    /// Intent of tests is to ensure Nodes and Relationships are created as expected when seeding data.
    /// IMPORTANT - WIPES the database during cleanup,
    /// so do NOT point at an important database! (hardcoded connection is intentional to help prevent accidentally wiping a DB)
    /// </summary>
    [TestFixture]
    public class SeedDataIntegrationTests
    {
        private IHost _host;
        private IDriver _driver;
        private IDataSeedService _seedService;

        [OneTimeSetUp]
        public async Task OneTimeSetup()
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.Debug()
                .CreateLogger();

            // Setup DI, configuration, and logging
            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Logging.AddSerilog(Log.Logger);

            builder.Configuration
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: true)
                .AddUserSecrets(typeof(SeedDataIntegrationTests).Assembly)
                .AddEnvironmentVariables();

            // Register Neo4j driver using configuration
            builder.Services.AddSingleton<IDriver>(_ =>
            {
                //var settings = new Neo4jSettings();
                //builder.Configuration.GetSection("Neo4jSettings").Bind(settings);

                // Deliberately using hardcoded values because this test wipes the DB during cleanup. Trying to avoid accidental wipe-age.
                var driver = GraphDatabase.Driver("neo4j://localhost:7687", 
                    AuthTokens.Basic("neo4j", "password-for-unit-testing-db-only"));

                Log.Information("neo4j driver initialized");
                Task.Run(() => Task.FromResult(driver.VerifyConnectivityAsync())).GetAwaiter().GetResult();
                Log.Information("neo4j connectivity verified");
                return driver;
            });

            builder.Services.AddSingleton<IDataRefreshPolicy, DataRefreshPolicy>();

            // Register node services dynamically
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
            builder.Services.AddSingleton<IDataSeedService, DataSeedService>();

            _host = builder.Build();
            _driver = _host.Services.GetRequiredService<IDriver>();
            _seedService = _host.Services.GetRequiredService<IDataSeedService>();

            _repo = _host.Services.GetRequiredService<INeo4jGenericRepo>();

            // Seed data during setup
            await _seedService.SeedAllData();
        }

        private INeo4jGenericRepo _repo;

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            await CleanupDatabase();
            await _driver.DisposeAsync();
            _repo.Dispose();
            _host.Dispose();
        }
        

        [Test]
        public async Task VerifyMoviesWereSeeded()
        {
            var movies = await _repo.LoadAllAsync<Movie>();

            Assert.That(movies, Is.Not.Empty, "Movies should have been seeded.");
            Assert.That(movies.Any(m => m.Title == "Toy Story"), "Toy Story should exist.");
            Assert.That(movies.Count, Is.GreaterThan(2));
        }

        [Test]
        public async Task VerifyGenresWereSeeded()
        {
            var genres = await _repo.LoadAllAsync<Genre>();

            Assert.That(genres, Is.Not.Empty, "Genres should have been seeded.");
            Assert.That(genres.Any(g => g.Name == "Action"), "Action genre should exist.");
            Assert.That(genres.Count, Is.GreaterThan(3));
        }

        [Test]
        public async Task VerifyPersonsWereSeeded()
        {
            var Persons = await _repo.LoadAllAsync<Person>();

            Assert.That(Persons, Is.Not.Empty, "Persons should have been seeded.");
            Assert.That(Persons.Any(g => g.Name == "Jane Smith"), "Jane Smith should exist.");
            Assert.That(Persons.Count, Is.GreaterThan(1));
        }


        [Test]
        public async Task VerifyPersonRelationshipsWereSeeded()
        {
            var person = await _repo.LoadAsync<Person>("1");

            Assert.That(person.ActedIn.Count, Is.EqualTo(1), "Person should have ACTED_IN 1 movie.");
            Assert.That(person.BirthYear, Is.EqualTo(1980));
        }

        [Test]
        public async Task VerifyMovieRelationshipsWereSeeded()
        {
            var toyStory = await _repo.LoadAsync<Movie>("1");
            Assert.That(toyStory, Is.Not.Null, "Toy Story should exist.");
            Assert.That(toyStory.GenreIds, Is.Not.Empty, "Toy Story should have genres.");
            Assert.That(toyStory.GenreIds.Contains("1"), "Toy Story should be in Animation genre.");
        }

        [Test]
        public async Task VerifyGenreRelationshipsWereSeeded()
        {
            var genres = await _repo.LoadAllAsync<Genre>();
            var animationGenre = genres.FirstOrDefault(g => g.Name == "Animation");
            Assert.That(animationGenre, Is.Not.Null, "Animation genre should exist.");
            Assert.That(animationGenre.HasMovies, Is.Not.Empty, "Animation genre should have movies.");
            Assert.That(animationGenre.HasMovies.Contains("1"), "Animation genre should include Toy Story.");
        }

        
        [Test]
        public async Task TestMovieEdgePropertyLoaded()
        {
            var toyStory = await _repo.LoadAsync<Movie>("1", true, [Movie.Edges.InGenre]);
            Assert.That(toyStory.InGenreEdges, Is.Not.Empty, "InGenre edges should be loaded");
            Assert.That(toyStory.InGenreEdges.Count, Is.EqualTo(2), "Toy Story should 2 genre edges.");
            var animationEdge = toyStory.InGenreEdges.FirstOrDefault(e => e.GetToId() == "1");
            Assert.That(animationEdge.SampleEdgeProperty, Is.EqualTo("toyStory-Animation"), "Toy Story should be in Animation genre.");
        }

        private async Task CleanupDatabase()
        {
            var session = _driver.AsyncSession();
            try
            {
                await session.RunAsync("MATCH (n) DETACH DELETE n");
            }
            finally
            {
                await session.CloseAsync();
            }
        }
    }
}