using Neo4jLiteRepo.Helpers;
using NUnit.Framework;

namespace Neo4jLiteRepo.UnitTests;

/// <summary>
/// Unit tests for ExtensionMethods - casing, sanitization, and slug generation.
/// </summary>
[TestFixture]
public class ExtensionMethodsTests
{
    #region ToPascalCase Tests

    [Test]
    public void ToPascalCase_FromCamelCase_ReturnsPascalCase()
    {
        // Act
        var result = "myPropertyName".ToPascalCase();

        // Assert
        Assert.That(result, Is.EqualTo("MyPropertyName"));
    }

    [Test]
    public void ToPascalCase_FromSnakeCase_ReturnsPascalCase()
    {
        // Act
        var result = "my_property_name".ToPascalCase();

        // Assert
        Assert.That(result, Is.EqualTo("MyPropertyName"));
    }

    [Test]
    public void ToPascalCase_FromAllCaps_ReturnsProperPascalCase()
    {
        // Act
        var result = "HTTP".ToPascalCase();

        // Assert
        Assert.That(result, Is.EqualTo("Http"));
    }

    [Test]
    public void ToPascalCase_WithSpaces_RemovesSpacesAndCapitalizes()
    {
        // Act
        var result = "my property name".ToPascalCase();

        // Assert
        Assert.That(result, Is.EqualTo("MyPropertyName"));
    }

    [Test]
    public void ToPascalCase_AlreadyPascalCase_ReturnsSameValue()
    {
        // Act
        var result = "Movie".ToPascalCase();

        // Assert
        Assert.That(result, Is.EqualTo("Movie"));
    }

    #endregion

    #region ToCamelCase Tests

    [Test]
    public void ToCamelCase_FromPascalCase_ReturnsCamelCase()
    {
        // Arrange - internal method, accessed via ToGraphPropertyCasing
        var result = "DisplayName".ToGraphPropertyCasing();

        // Assert
        Assert.That(result, Is.EqualTo("displayName"));
    }

    [Test]
    public void ToCamelCase_FromSnakeCase_ReturnsCamelCase()
    {
        // Act
        var result = "my_property".ToGraphPropertyCasing();

        // Assert
        Assert.That(result, Is.EqualTo("myProperty"));
    }

    #endregion

    #region ToGraphPropertyCasing Tests

    [Test]
    public void ToGraphPropertyCasing_EmptyString_ReturnsEmpty()
    {
        // Act
        var result = "".ToGraphPropertyCasing();

        // Assert
        Assert.That(result, Is.EqualTo(string.Empty));
    }

    [Test]
    public void ToGraphPropertyCasing_WhitespaceOnly_ReturnsEmpty()
    {
        // Act
        var result = "   ".ToGraphPropertyCasing();

        // Assert
        Assert.That(result, Is.EqualTo(string.Empty));
    }

    [Test]
    public void ToGraphPropertyCasing_NormalProperty_ReturnsCamelCase()
    {
        // Act
        var result = "Title".ToGraphPropertyCasing();

        // Assert
        Assert.That(result, Is.EqualTo("title"));
    }

    #endregion

    #region ToGraphRelationShipCasing Tests

    [Test]
    public void ToGraphRelationShipCasing_LowerCase_ReturnsUpperCase()
    {
        // Act
        var result = "in_genre".ToGraphRelationShipCasing();

        // Assert
        Assert.That(result, Is.EqualTo("IN_GENRE"));
    }

    [Test]
    public void ToGraphRelationShipCasing_MixedCase_ReturnsUpperCase()
    {
        // Act
        var result = "Has_Movie".ToGraphRelationShipCasing();

        // Assert
        Assert.That(result, Is.EqualTo("HAS_MOVIE"));
    }

    #endregion

    #region SanitizeForCypher Tests

    [Test]
    public void SanitizeForCypher_NullValue_ReturnsNull()
    {
        // Arrange
        string? value = null;

        // Act
        var result = value.SanitizeForCypher();

        // Assert
        Assert.That(result, Is.Null);
    }

    [Test]
    public void SanitizeForCypher_StringWithBackslash_EscapesBackslash()
    {
        // Arrange
        var value = "path\\to\\file";

        // Act
        var result = value.SanitizeForCypher();

        // Assert
        Assert.That(result, Is.EqualTo("path\\\\to\\\\file"));
    }

    [Test]
    public void SanitizeForCypher_StringWithDoubleQuotes_EscapesQuotes()
    {
        // Arrange
        var value = "He said \"hello\"";

        // Act
        var result = value.SanitizeForCypher();

        // Assert
        Assert.That(result, Is.EqualTo("He said \\\"hello\\\""));
    }

    [Test]
    public void SanitizeForCypher_StringWithBothSpecialChars_EscapesBoth()
    {
        // Arrange - Tests injection prevention
        var value = "\\\" OR 1=1 --";

        // Act
        var result = value.SanitizeForCypher();

        // Assert
        Assert.That(result, Is.EqualTo("\\\\\\\" OR 1=1 --"));
    }

    [Test]
    public void SanitizeForCypher_NormalString_ReturnsUnchanged()
    {
        // Arrange
        var value = "Normal text without special chars";

        // Act
        var result = value.SanitizeForCypher();

        // Assert
        Assert.That(result, Is.EqualTo("Normal text without special chars"));
    }

    [Test]
    public void SanitizeForCypher_ObjectOverload_NullObject_ReturnsNull()
    {
        // Arrange
        object? value = null;

        // Act
        var result = value.SanitizeForCypher();

        // Assert
        Assert.That(result, Is.Null);
    }

    [Test]
    public void SanitizeForCypher_ObjectOverload_WithSpecialChars_Sanitizes()
    {
        // Arrange
        object value = "test\"value";

        // Act
        var result = value.SanitizeForCypher();

        // Assert
        Assert.That(result, Is.EqualTo("test\\\"value"));
    }

    #endregion

    #region Slugify Tests

    [Test]
    public void Slugify_NormalText_ReturnsSlug()
    {
        // Act
        var result = "Hello World".Slugify();

        // Assert
        Assert.That(result, Is.EqualTo("hello-world"));
    }

    [Test]
    public void Slugify_WithSpecialCharacters_RemovesThem()
    {
        // Act
        var result = "Hello! World? (2024)".Slugify();

        // Assert
        Assert.That(result, Is.EqualTo("hello-world-2024"));
    }

    [Test]
    public void Slugify_WithDiacritics_RemovesAccents()
    {
        // Act
        var result = "café résumé".Slugify();

        // Assert
        Assert.That(result, Is.EqualTo("cafe-resume"));
    }

    [Test]
    public void Slugify_WithMultipleSpaces_CollapsesToSingleDash()
    {
        // Act
        var result = "Hello    World".Slugify();

        // Assert
        Assert.That(result, Is.EqualTo("hello-world"));
    }

    #endregion

    #region ExtractLastSegment Tests

    [Test]
    public void ExtractLastSegment_WithSlashes_ReturnsLastPart()
    {
        // Act
        var result = "path/to/file.txt".ExtractLastSegment();

        // Assert
        Assert.That(result, Is.EqualTo("file.txt"));
    }

    [Test]
    public void ExtractLastSegment_EmptyString_ReturnsNullReplacement()
    {
        // Act
        var result = "".ExtractLastSegment();

        // Assert
        Assert.That(result, Is.EqualTo("none"));
    }

    [Test]
    public void ExtractLastSegment_NullString_ReturnsNullReplacement()
    {
        // Act
        var result = ((string)null!).ExtractLastSegment();

        // Assert
        Assert.That(result, Is.EqualTo("none"));
    }

    [Test]
    public void ExtractLastSegment_CustomDelimiter_SplitsCorrectly()
    {
        // Act
        var result = "a::b::last".ExtractLastSegment("::");

        // Assert
        Assert.That(result, Is.EqualTo("last"));
    }

    #endregion

    #region IsBool Tests

    [Test]
    public void IsBool_TrueString_ReturnsTrue()
    {
        // Act
        var result = ((object)"true").IsBool();

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public void IsBool_FalseString_ReturnsTrue()
    {
        // Act
        var result = ((object)"false").IsBool();

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public void IsBool_NonBoolString_ReturnsFalse()
    {
        // Act
        var result = ((object)"notabool").IsBool();

        // Assert
        Assert.That(result, Is.False);
    }

    #endregion
}
