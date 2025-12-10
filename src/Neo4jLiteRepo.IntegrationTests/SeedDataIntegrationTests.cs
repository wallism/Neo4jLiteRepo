using Microsoft.Extensions.DependencyInjection;
using Neo4jLiteRepo.Sample.Nodes;
using NUnit.Framework;

namespace Neo4jLiteRepo.Tests.Integration;

/// <summary>
/// Intent of tests is to ensure Nodes and Relationships are created as expected when seeding data.
/// </summary>
[TestFixture]
public class SeedDataIntegrationTests : Neo4jIntegrationTestBase
{
    private IDataSeedService _seedService = null!;

    /// <summary>
    /// Use the default neo4j database for seed tests (Community Edition compatible).
    /// </summary>
    protected override string TestDatabase => "neo4j";

    protected override async Task OnPostSetupAsync()
    {
        _seedService = Host.Services.GetRequiredService<IDataSeedService>();

        // Seed data during setup
        await _seedService.SeedAllData();
    }

    [Test]
    public async Task VerifyMoviesWereSeeded()
    {
        var movies = await Repo.LoadAllAsync<Movie>();

        Assert.That(movies, Is.Not.Empty, "Movies should have been seeded.");
        Assert.That(movies.Any(m => m.Title == "Toy Story"), "Toy Story should exist.");
        Assert.That(movies.Count, Is.GreaterThan(2));
    }

    [Test]
    public async Task VerifyGenresWereSeeded()
    {
        var genres = await Repo.LoadAllAsync<Genre>();

        Assert.That(genres, Is.Not.Empty, "Genres should have been seeded.");
        Assert.That(genres.Any(g => g.Name == "Action"), "Action genre should exist.");
        Assert.That(genres.Count, Is.GreaterThan(3));
    }

    [Test]
    public async Task VerifyPersonsWereSeeded()
    {
        var Persons = await Repo.LoadAllAsync<Person>();

        Assert.That(Persons, Is.Not.Empty, "Persons should have been seeded.");
        Assert.That(Persons.Any(g => g.Name == "Jane Smith"), "Jane Smith should exist.");
        Assert.That(Persons.Count, Is.GreaterThan(1));
    }

    [Test]
    public async Task VerifyPersonRelationshipsWereSeeded()
    {
        var person = await Repo.LoadAsync<Person>("1");

        Assert.That(person.ActedIn.Count, Is.EqualTo(1), "Person should have ACTED_IN 1 movie.");
        Assert.That(person.BirthYear, Is.EqualTo(1980));
    }

    [Test]
    public async Task VerifyMovieRelationshipsWereSeeded()
    {
        var toyStory = await Repo.LoadAsync<Movie>("1");

        Assert.That(toyStory, Is.Not.Null, "Toy Story should exist.");
        Assert.That(toyStory.GenreIds, Is.Not.Empty, "Toy Story should have genres.");
        Assert.That(toyStory.GenreIds.Contains("1"), "Toy Story should be in Animation genre.");
    }

    [Test]
    public async Task VerifyGenreRelationshipsWereSeeded()
    {
        var genres = await Repo.LoadAllAsync<Genre>();
        var animationGenre = genres.FirstOrDefault(g => g.Name == "Animation");

        Assert.That(animationGenre, Is.Not.Null, "Animation genre should exist.");
        Assert.That(animationGenre.HasMovies, Is.Not.Empty, "Animation genre should have movies.");
        Assert.That(animationGenre.HasMovies.Contains("1"), "Animation genre should include Toy Story.");
    }

    [Test]
    public async Task TestMovieEdgePropertyLoaded()
    {
        var toyStory = await Repo.LoadAsync<Movie>("1", true, [Movie.Edges.InGenre]);

        Assert.That(toyStory.InGenreEdges, Is.Not.Empty, "InGenre edges should be loaded");
        Assert.That(toyStory.InGenreEdges.Count, Is.EqualTo(2), "Toy Story should 2 genre edges.");
        var animationEdge = toyStory.InGenreEdges.FirstOrDefault(e => e.GetToId() == "1");
        Assert.That(animationEdge.SampleEdgeProperty, Is.EqualTo("toyStory-Animation"), "Toy Story should be in Animation genre.");
    }

    [Test]
    public async Task VerifyGetAllNodesAndRelationshipsAsync()
    {
        // Arrange
        var session = Driver.AsyncSession();

        // Act
        var response = await Repo.GetAllNodesAndEdgesAsync(session);

        // Assert
        Assert.That(response, Is.Not.Null, "Response should not be null.");
        Assert.That(response.NodeTypes, Is.Not.Empty, "Node types should not be empty.");

        foreach (var nodeType in response.NodeTypes)
        {
            Assert.That(nodeType.NodeType, Is.Not.Null.Or.Empty, "NodeType should not be null or empty.");
            Assert.That(nodeType.OutgoingRelationships, Is.Not.Null, "OutgoingRelationships should not be null.");
            Assert.That(nodeType.IncomingRelationships, Is.Not.Null, "IncomingRelationships should not be null.");
        }

        // Additional assertion for nodeType "Movie"
        var movieNode = response.NodeTypes.FirstOrDefault(n => n.NodeType == "Movie");
        Assert.That(movieNode, Is.Not.Null, "Movie node type should exist.");
        Assert.That(movieNode.IncomingRelationships.Count, Is.EqualTo(2), "Movie node should have exactly two incoming relationships.");
        Assert.That(movieNode.OutgoingRelationships.Count, Is.EqualTo(1), "Movie node should have exactly one outgoing relationship.");
    }
}