using Neo4jLiteRepo.Attributes;
using Neo4jLiteRepo.Sample.Nodes;
using NUnit.Framework;

namespace Neo4jLiteRepo.UnitTests;

/// <summary>
/// Unit tests for GraphNode base class - primary key reflection, label naming, and display name logic.
/// </summary>
[TestFixture]
public class GraphNodeTests
{
    #region GetPrimaryKeyName Tests

    [Test]
    public void GetPrimaryKeyName_ReturnsPropertyNameInCamelCase()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test" };

        // Act
        var pkName = movie.GetPrimaryKeyName();

        // Assert
        Assert.That(pkName, Is.EqualTo("id"));
    }

    [Test]
    public void GetPrimaryKeyName_Static_ReturnsCorrectName()
    {
        // Act
        var pkName = GraphNode.GetPrimaryKeyName<Movie>();

        // Assert
        Assert.That(pkName, Is.EqualTo("id"));
    }

    #endregion

    #region GetPrimaryKeyValue Tests

    [Test]
    public void GetPrimaryKeyValue_ReturnsPropertyValue()
    {
        // Arrange
        var movie = new Movie { Id = "abc-123", Title = "Test" };

        // Act
        var pkValue = movie.GetPrimaryKeyValue();

        // Assert
        Assert.That(pkValue, Is.EqualTo("abc-123"));
    }

    [Test]
    public void GetPrimaryKeyValue_WhenNull_ThrowsInvalidOperationException()
    {
        // Arrange
        var movie = new Movie { Id = null!, Title = "Test" };

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => movie.GetPrimaryKeyValue());
    }

    #endregion

    #region LabelName Tests

    [Test]
    public void LabelName_ReturnsPascalCaseClassName()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test" };

        // Act
        var label = movie.LabelName;

        // Assert
        Assert.That(label, Is.EqualTo("Movie"));
    }

    [Test]
    public void GetLabelName_Static_ReturnsClassName()
    {
        // Act
        var label = GraphNode.GetLabelName<Genre>();

        // Assert
        Assert.That(label, Is.EqualTo("Genre"));
    }

    #endregion

    #region BuildDisplayName Tests

    [Test]
    public void BuildDisplayName_ReturnsImplementedValue()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "The Matrix" };

        // Act
        var displayName = movie.BuildDisplayName();

        // Assert
        Assert.That(displayName, Is.EqualTo("The Matrix"));
    }

    [Test]
    public void DisplayName_Property_DelegatesToBuildDisplayName()
    {
        // Arrange
        var genre = new Genre { Id = "g1", Name = "Action" };

        // Act & Assert
        Assert.That(genre.DisplayName, Is.EqualTo("Action"));
    }

    [Test]
    public void ToString_ReturnsDisplayName()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Inception" };

        // Act
        var result = movie.ToString();

        // Assert
        Assert.That(result, Is.EqualTo("Inception"));
    }

    #endregion

    #region EnforceUniqueConstraint Tests

    [Test]
    public void EnforceUniqueConstraint_DefaultsToTrue()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test" };

        // Act & Assert
        Assert.That(movie.EnforceUniqueConstraint, Is.True);
    }

    #endregion

    #region Multiple PrimaryKey Validation

    [Test]
    public void GetPrimaryKeyName_WithAbstractClassPK_ThrowsInvalidOperationException()
    {
        // Arrange - a node class that has PK declared on abstract base
        var node = new NodeWithAbstractPk();

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => node.GetPrimaryKeyName());
    }

    #endregion

    #region NodeDisplayNameProperty Tests

    [Test]
    public void NodeDisplayNameProperty_ReturnsCamelCaseDisplayName()
    {
        // Arrange
        var movie = new Movie { Id = "1", Title = "Test" };

        // Act
        var prop = movie.NodeDisplayNameProperty;

        // Assert
        Assert.That(prop, Is.EqualTo("displayName"));
    }

    #endregion

    #region Test Helpers

    /// <summary>
    /// Abstract class that incorrectly defines PK at the abstract level
    /// </summary>
    private abstract class AbstractNodeWithPk : GraphNode
    {
        [NodePrimaryKey]
        public string BaseId { get; set; } = string.Empty;

        public override string GetMainContent() => BaseId;
    }

    /// <summary>
    /// Concrete class that also defines PK - triggers validation error
    /// </summary>
    private class NodeWithAbstractPk : AbstractNodeWithPk
    {
        [NodePrimaryKey]
        public string ConcreteId { get; set; } = "test";

        public override string BuildDisplayName() => ConcreteId;
    }

    #endregion
}
