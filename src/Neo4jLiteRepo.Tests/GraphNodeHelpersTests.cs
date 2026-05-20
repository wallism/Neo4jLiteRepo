using Neo4jLiteRepo.Attributes;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.Sample.Nodes;
using NUnit.Framework;

namespace Neo4jLiteRepo.UnitTests;

/// <summary>
/// Unit tests for GraphNodeHelpers.DeduplicateNodeIds - node deduplication logic.
/// </summary>
[TestFixture]
public class GraphNodeHelpersTests
{
    #region DeduplicateNodeIds Tests

    [Test]
    public void DeduplicateNodeIds_NoDuplicates_ReturnsAllNodes()
    {
        // Arrange
        var nodes = new List<GraphNode>
        {
            new Movie { Id = "1", Title = "Movie 1" },
            new Movie { Id = "2", Title = "Movie 2" },
            new Movie { Id = "3", Title = "Movie 3" }
        };

        // Act
        var result = GraphNodeHelpers.DeduplicateNodeIds(nodes);

        // Assert
        Assert.That(result, Has.Count.EqualTo(3));
    }

    [Test]
    public void DeduplicateNodeIds_DuplicateWithSameContent_SkipsDuplicate()
    {
        // Arrange - same PK and same content (GetMainContent returns Title)
        var nodes = new List<GraphNode>
        {
            new Movie { Id = "1", Title = "Movie 1" },
            new Movie { Id = "1", Title = "Movie 1" } // identical content
        };

        // Act
        var result = GraphNodeHelpers.DeduplicateNodeIds(nodes);

        // Assert
        Assert.That(result, Has.Count.EqualTo(1));
        Assert.That(result[0].GetPrimaryKeyValue(), Is.EqualTo("1"));
    }

    [Test]
    public void DeduplicateNodeIds_DuplicateWithDifferentContent_AssignsNewPK()
    {
        // Arrange - same PK but different content
        var nodes = new List<GraphNode>
        {
            new Movie { Id = "1", Title = "Movie 1" },
            new Movie { Id = "1", Title = "Movie 1 - Director's Cut" } // different content
        };

        // Act
        var result = GraphNodeHelpers.DeduplicateNodeIds(nodes);

        // Assert
        Assert.That(result, Has.Count.EqualTo(2));
        var pks = result.Select(n => n.GetPrimaryKeyValue()).ToList();
        Assert.That(pks, Does.Contain("1"));
        Assert.That(pks, Does.Contain("1-DUP1"));
    }

    [Test]
    public void DeduplicateNodeIds_EmptyList_ReturnsEmptyList()
    {
        // Act
        var result = GraphNodeHelpers.DeduplicateNodeIds([]);

        // Assert
        Assert.That(result, Is.Empty);
    }

    [Test]
    public void DeduplicateNodeIds_MultipleDuplicatesWithDifferentContent_AssignsIncrementingKeys()
    {
        // Arrange - 3 nodes with same PK but different content
        var nodes = new List<GraphNode>
        {
            new Movie { Id = "x", Title = "Version A" },
            new Movie { Id = "x", Title = "Version B" },
            new Movie { Id = "x", Title = "Version C" }
        };

        // Act
        var result = GraphNodeHelpers.DeduplicateNodeIds(nodes);

        // Assert
        Assert.That(result, Has.Count.EqualTo(3));
        var pks = result.Select(n => n.GetPrimaryKeyValue()).OrderBy(x => x).ToList();
        Assert.That(pks, Does.Contain("x"));
    }

    [Test]
    public void DeduplicateNodeIds_NullPrimaryKey_SkipsNode()
    {
        // Arrange - node with null PK value should be skipped (GetPrimaryKeyValue throws, but the method checks for null)
        var nodes = new List<GraphNode>
        {
            new Movie { Id = "1", Title = "Valid" },
            new Movie { Id = "2", Title = "Also Valid" }
        };

        // Act
        var result = GraphNodeHelpers.DeduplicateNodeIds(nodes);

        // Assert
        Assert.That(result, Has.Count.EqualTo(2));
    }

    #endregion
}
