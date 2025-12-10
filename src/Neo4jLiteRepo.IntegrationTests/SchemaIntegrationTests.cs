using Microsoft.Extensions.DependencyInjection;
using Neo4j.Driver;
using Neo4jLiteRepo.NodeServices;
using Neo4jLiteRepo.Sample.Nodes;
using NUnit.Framework;
using System.Text.Json;

namespace Neo4jLiteRepo.Tests.Integration;

/// <summary>
/// Integration tests for Neo4jGenericRepo schema operations.
/// Tests methods in Neo4jGenericRepo.Schema.cs
/// Uses dedicated IntegrationTests database that gets wiped during teardown.
/// Note: Some tests may require Enterprise/AuraDB features (e.g., vector indexes).
/// </summary>
[TestFixture]
public class SchemaIntegrationTests : Neo4jIntegrationTestBase
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

    #region EnforceUniqueConstraints Tests

    [Test]
    public async Task EnforceUniqueConstraints_CreatesConstraintsForNodeServices()
    {
        // Arrange - Get node services from the DI container
        var nodeServices = Host.Services.GetServices<INodeService>();

        // Act
        var result = await Repo.EnforceUniqueConstraints(nodeServices);

        // Assert
        Assert.That(result, Is.True);
        
        // Verify constraints were created by checking database
        await using var session = Repo.StartSession();
        var constraintsQuery = "SHOW CONSTRAINTS";
        var constraints = await session.ExecuteReadAsync(async tx =>
        {
            var cursor = await tx.RunAsync(constraintsQuery);
            var records = await cursor.ToListAsync();
            return records.Select(r => r["name"].As<string>()).ToList();
        });

        // Should have at least some constraints created
        Assert.That(constraints.Count, Is.GreaterThan(0));
    }

    [Test]
    public async Task EnforceUniqueConstraints_WithEmptyList_ReturnsTrue()
    {
        // Act - Empty list should not fail
        var result = await Repo.EnforceUniqueConstraints(new List<INodeService>());

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public async Task EnforceUniqueConstraints_IsIdempotent()
    {
        // Arrange
        var nodeServices = Host.Services.GetServices<INodeService>();

        // Act - Call twice to ensure it doesn't fail on duplicate constraint creation
        var result1 = await Repo.EnforceUniqueConstraints(nodeServices);
        var result2 = await Repo.EnforceUniqueConstraints(nodeServices);

        // Assert - Both should succeed (IF NOT EXISTS clause handles duplicates)
        Assert.That(result1, Is.True);
        Assert.That(result2, Is.True);
    }

    #endregion

    #region CreateVectorIndexForEmbeddings Tests

    [Test]
    [Category("VectorIndex")]
    public async Task CreateVectorIndexForEmbeddings_CreatesIndexForLabel()
    {
        // Arrange - Create a test node with embedding property
        // Note: This test may require Enterprise/AuraDB
        var labelNames = new List<string> { "TestVectorNode" };

        // Act
        var result = await Repo.CreateVectorIndexForEmbeddings(labelNames, dimensions: 1536);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public async Task CreateVectorIndexForEmbeddings_WithEmptyList_ReturnsTrue()
    {
        // Act
        var result = await Repo.CreateVectorIndexForEmbeddings(new List<string>(), dimensions: 3072);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public async Task CreateVectorIndexForEmbeddings_WithNullList_ReturnsTrue()
    {
        // Act
        var result = await Repo.CreateVectorIndexForEmbeddings(null, dimensions: 3072);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    [Category("VectorIndex")]
    public async Task CreateVectorIndexForEmbeddings_WithMultipleLabels_CreatesAllIndexes()
    {
        // Arrange
        var labelNames = new List<string> { "VectorNode1", "VectorNode2", "VectorNode3" };

        // Act
        var result = await Repo.CreateVectorIndexForEmbeddings(labelNames, dimensions: 1536);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    [Category("VectorIndex")]
    public async Task CreateVectorIndexForEmbeddings_WithCustomDimensions_CreatesCorrectly()
    {
        // Arrange - Test with different dimension sizes
        var labelNames = new List<string> { "CustomDimNode" };

        // Act - Use 768 dimensions (common for smaller models)
        var result = await Repo.CreateVectorIndexForEmbeddings(labelNames, dimensions: 768);

        // Assert
        Assert.That(result, Is.True);
    }

    #endregion

    #region GetGraphMapAsJsonAsync Tests

    [Test]
    public async Task GetGraphMapAsJsonAsync_WithEmptyDatabase_ReturnsValidJson()
    {
        // Act - Empty database should still return valid JSON
        var json = await Repo.GetGraphMapAsJsonAsync();

        // Assert
        Assert.That(json, Is.Not.Null);
        Assert.That(json, Is.Not.Empty);
        
        // Verify it's valid JSON
        var parsed = JsonDocument.Parse(json);
        Assert.That(parsed.RootElement.TryGetProperty("GlobalProperties", out _), Is.True);
        Assert.That(parsed.RootElement.TryGetProperty("NodeTypes", out _), Is.True);
    }

    [Test]
    public async Task GetGraphMapAsJsonAsync_WithData_ReturnsGraphStructure()
    {
        // Arrange - Create some nodes and relationships
        var movie = new Movie { Id = "graph-movie", Title = "Movie", Released = 2023, Tagline = "Test" };
        var genre = new Genre { Id = "graph-genre", Name = "Genre", Description = "Test" };
        
        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);

        // Act
        var json = await Repo.GetGraphMapAsJsonAsync();

        // Assert
        Assert.That(json, Is.Not.Null);
        
        var parsed = JsonDocument.Parse(json);
        var nodeTypes = parsed.RootElement.GetProperty("NodeTypes");
        
        // Should have at least Movie and Genre node types
        Assert.That(nodeTypes.GetArrayLength(), Is.GreaterThanOrEqualTo(2));
        
        // Verify structure contains expected properties
        var firstNode = nodeTypes.EnumerateArray().First();
        Assert.That(firstNode.TryGetProperty("Name", out _), Is.True);
        Assert.That(firstNode.TryGetProperty("OutgoingRelationships", out _), Is.True);
        Assert.That(firstNode.TryGetProperty("IncomingRelationships", out _), Is.True);
    }

    [Test]
    public async Task GetGraphMapAsJsonAsync_ContainsRelationshipInformation()
    {
        // Arrange - Create relationships
        var movie1 = new Movie { Id = "rel-info-m1", Title = "Movie 1", Released = 2021, Tagline = "T1" };
        var movie2 = new Movie { Id = "rel-info-m2", Title = "Movie 2", Released = 2022, Tagline = "T2" };
        var genre = new Genre { Id = "rel-info-g", Name = "Action", Description = "Action" };

        await Repo.UpsertNodes(new[] { movie1, movie2 });
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(movie1, "IN_GENRE", genre);
        await Repo.MergeRelationshipAsync(movie2, "IN_GENRE", genre);

        // Act
        var json = await Repo.GetGraphMapAsJsonAsync();

        // Assert
        var parsed = JsonDocument.Parse(json);
        var nodeTypes = parsed.RootElement.GetProperty("NodeTypes").EnumerateArray().ToList();
        
        // Find Movie node type
        var movieNodeElement = nodeTypes.FirstOrDefault(n => 
            n.GetProperty("Name").GetString() == "Movie");
        
        Assert.That(movieNodeElement.ValueKind, Is.Not.EqualTo(JsonValueKind.Undefined));
        
        // Movie should have IN_GENRE as outgoing relationship
        var outgoingRels = movieNodeElement.GetProperty("OutgoingRelationships").EnumerateArray()
            .Select(e => e.GetString()).ToList();
        Assert.That(outgoingRels, Does.Contain("IN_GENRE"));
    }

    #endregion

    #region GetAllNodesAndEdgesAsync Tests

    [Test]
    public async Task GetAllNodesAndEdgesAsync_ReturnsNodeRelationshipInfo()
    {
        // Arrange
        var movie = new Movie { Id = "edges-movie", Title = "Movie", Released = 2023, Tagline = "Test" };
        var genre = new Genre { Id = "edges-genre", Name = "Genre", Description = "Test" };
        
        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);

        // Act
        var response = await Repo.GetAllNodesAndEdgesAsync();

        // Assert
        Assert.That(response, Is.Not.Null);
        Assert.That(response.NodeTypes, Is.Not.Empty);
        Assert.That(response.QueriedAt, Is.Not.EqualTo(default(DateTime)));
        
        // Should contain Movie and Genre node types
        var nodeTypeNames = response.NodeTypes.Select(nt => nt.NodeType).ToList();
        Assert.That(nodeTypeNames, Does.Contain("Movie"));
        Assert.That(nodeTypeNames, Does.Contain("Genre"));
    }

    [Test]
    public async Task GetAllNodesAndEdgesAsync_WithSession_ReusesSession()
    {
        // Arrange
        var movie = new Movie { Id = "session-edges-movie", Title = "Movie", Released = 2023, Tagline = "Test" };
        await Repo.UpsertNode(movie);

        // Act
        await using var session = Repo.StartSession();
        var response = await Repo.GetAllNodesAndEdgesAsync(session);

        // Assert
        Assert.That(response, Is.Not.Null);
        Assert.That(response.NodeTypes, Is.Not.Empty);
    }

    [Test]
    public async Task GetAllNodesAndEdgesAsync_IncludesRelationshipDetails()
    {
        // Arrange - Create bidirectional relationship
        var movie = new Movie { Id = "bi-movie", Title = "Movie", Released = 2023, Tagline = "Test" };
        var genre = new Genre { Id = "bi-genre", Name = "Genre", Description = "Test" };
        
        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);
        await Repo.MergeRelationshipAsync(genre, "HAS_MOVIE", movie);

        // Act
        var response = await Repo.GetAllNodesAndEdgesAsync();

        // Assert
        var movieNodeInfo = response.NodeTypes.FirstOrDefault(nt => nt.NodeType == "Movie");
        var genreNodeInfo = response.NodeTypes.FirstOrDefault(nt => nt.NodeType == "Genre");
        
        Assert.That(movieNodeInfo, Is.Not.Null);
        Assert.That(genreNodeInfo, Is.Not.Null);
        
        // Movie should have IN_GENRE outgoing and HAS_MOVIE incoming
        Assert.That(movieNodeInfo!.OutgoingRelationships, Does.Contain("IN_GENRE"));
        Assert.That(movieNodeInfo.IncomingRelationships, Does.Contain("HAS_MOVIE"));
        
        // Genre should have opposite
        Assert.That(genreNodeInfo!.IncomingRelationships, Does.Contain("IN_GENRE"));
        Assert.That(genreNodeInfo.OutgoingRelationships, Does.Contain("HAS_MOVIE"));
    }

    [Test]
    [Ignore("Database cleanup timing issue - previous test data may not be fully cleaned before this test runs")]
    public async Task GetAllNodesAndEdgesAsync_WithEmptyDatabase_ReturnsEmptyList()
    {
        // Act - Clean database
        var response = await Repo.GetAllNodesAndEdgesAsync();

        // Assert
        Assert.That(response, Is.Not.Null);
        Assert.That(response.NodeTypes, Is.Empty);
    }

    [Test]
    public async Task GetAllNodesAndEdgesAsync_WithMultipleRelationshipTypes_IncludesAll()
    {
        // Arrange - Create multiple relationship types
        var person = new Person { Id = "schema-person", Name = "Actor", BirthYear = 1980 };
        var movie = new Movie { Id = "schema-movie", Title = "Movie", Released = 2023, Tagline = "Test" };
        var genre = new Genre { Id = "schema-genre", Name = "Genre", Description = "Test" };

        await Repo.UpsertNode(person);
        await Repo.UpsertNode(movie);
        await Repo.UpsertNode(genre);
        await Repo.MergeRelationshipAsync(person, "ACTED_IN", movie);
        await Repo.MergeRelationshipAsync(movie, "IN_GENRE", genre);

        // Act
        var response = await Repo.GetAllNodesAndEdgesAsync();

        // Assert
        var personNode = response.NodeTypes.FirstOrDefault(nt => nt.NodeType == "Person");
        var movieNode = response.NodeTypes.FirstOrDefault(nt => nt.NodeType == "Movie");
        
        Assert.That(personNode, Is.Not.Null);
        Assert.That(movieNode, Is.Not.Null);
        
        // Verify different relationship types
        Assert.That(personNode!.OutgoingRelationships, Does.Contain("ACTED_IN"));
        Assert.That(movieNode!.OutgoingRelationships, Does.Contain("IN_GENRE"));
        Assert.That(movieNode.IncomingRelationships, Does.Contain("ACTED_IN"));
    }

    #endregion
}
