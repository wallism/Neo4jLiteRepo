using Neo4jLiteRepo.Helpers;
using NUnit.Framework;

namespace Neo4jLiteRepo.UnitTests;

/// <summary>
/// Unit tests for ValueConversionExtensions - type conversion helpers for Neo4j value mapping.
/// </summary>
[TestFixture]
public class ValueConversionExtensionsTests
{
    #region ConvertToFloatArray Tests

    [Test]
    public void ConvertToFloatArray_FromFloatArray_ReturnsSameArray()
    {
        // Arrange
        float[] input = [1.0f, 2.5f, 3.7f];

        // Act
        var result = ((object)input).ConvertToFloatArray();

        // Assert
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public void ConvertToFloatArray_FromDoubleArray_ConvertsToFloat()
    {
        // Arrange
        double[] input = [1.0, 2.5, 3.7];

        // Act
        var result = ((object)input).ConvertToFloatArray();

        // Assert
        Assert.That(result, Has.Length.EqualTo(3));
        Assert.That(result[0], Is.EqualTo(1.0f));
        Assert.That(result[1], Is.EqualTo(2.5f));
        Assert.That(result[2], Is.EqualTo(3.7f));
    }

    [Test]
    public void ConvertToFloatArray_FromObjectEnumerable_ConvertsToFloat()
    {
        // Arrange
        var input = new List<object> { 1.0, 2.5, 3.7 };

        // Act
        var result = ((object)input).ConvertToFloatArray();

        // Assert
        Assert.That(result, Has.Length.EqualTo(3));
    }

    [Test]
    public void ConvertToFloatArray_UnsupportedType_ReturnsEmptyArray()
    {
        // Arrange
        object input = "not an array";

        // Act
        var result = input.ConvertToFloatArray();

        // Assert
        Assert.That(result, Is.Empty);
    }

    #endregion

    #region ConvertToGuid Tests

    [Test]
    public void ConvertToGuid_FromGuid_ReturnsSameGuid()
    {
        // Arrange
        var expected = Guid.NewGuid();

        // Act
        var result = ((object)expected).ConvertToGuid();

        // Assert
        Assert.That(result, Is.EqualTo(expected));
    }

    [Test]
    public void ConvertToGuid_FromValidString_ParsesGuid()
    {
        // Arrange
        var guidString = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";

        // Act
        var result = ((object)guidString).ConvertToGuid();

        // Assert
        Assert.That(result, Is.EqualTo(Guid.Parse(guidString)));
    }

    [Test]
    public void ConvertToGuid_FromInvalidString_ReturnsEmpty()
    {
        // Act
        var result = ((object)"not-a-guid").ConvertToGuid();

        // Assert
        Assert.That(result, Is.EqualTo(Guid.Empty));
    }

    [Test]
    public void ConvertToGuid_FromNonStringNonGuid_ReturnsEmpty()
    {
        // Act
        var result = ((object)12345).ConvertToGuid();

        // Assert
        Assert.That(result, Is.EqualTo(Guid.Empty));
    }

    #endregion

    #region ConvertToDateTimeOffset Tests

    [Test]
    public void ConvertToDateTimeOffset_FromDateTimeOffset_ReturnsSameValue()
    {
        // Arrange
        var expected = new DateTimeOffset(2024, 6, 15, 10, 30, 0, TimeSpan.FromHours(2));

        // Act
        var result = ((object)expected).ConvertToDateTimeOffset();

        // Assert
        Assert.That(result, Is.EqualTo(expected));
    }

    [Test]
    public void ConvertToDateTimeOffset_FromDateTime_ConvertsToUtc()
    {
        // Arrange
        var dt = new DateTime(2024, 6, 15, 10, 30, 0, DateTimeKind.Utc);

        // Act
        var result = ((object)dt).ConvertToDateTimeOffset();

        // Assert
        Assert.That(result.UtcDateTime, Is.EqualTo(dt));
    }

    [Test]
    public void ConvertToDateTimeOffset_FromUnspecifiedDateTime_TreatsAsUtc()
    {
        // Arrange
        var dt = new DateTime(2024, 6, 15, 10, 30, 0, DateTimeKind.Unspecified);

        // Act
        var result = ((object)dt).ConvertToDateTimeOffset();

        // Assert
        Assert.That(result.Offset, Is.EqualTo(TimeSpan.Zero));
    }

    [Test]
    public void ConvertToDateTimeOffset_FromValidString_ParsesCorrectly()
    {
        // Arrange
        object input = "2024-06-15T10:30:00Z";

        // Act
        var result = input.ConvertToDateTimeOffset();

        // Assert
        Assert.That(result.Year, Is.EqualTo(2024));
        Assert.That(result.Month, Is.EqualTo(6));
        Assert.That(result.Day, Is.EqualTo(15));
    }

    [Test]
    public void ConvertToDateTimeOffset_FromInvalidValue_ReturnsMinValue()
    {
        // Act
        var result = ((object)"not-a-date").ConvertToDateTimeOffset();

        // Assert
        Assert.That(result, Is.EqualTo(DateTimeOffset.MinValue));
    }

    #endregion

    #region ConvertToDateTime Tests

    [Test]
    public void ConvertToDateTime_FromDateTime_ReturnsSameValue()
    {
        // Arrange
        var expected = new DateTime(2024, 1, 15, 8, 0, 0, DateTimeKind.Utc);

        // Act
        var result = ((object)expected).ConvertToDateTime();

        // Assert
        Assert.That(result, Is.EqualTo(expected));
    }

    [Test]
    public void ConvertToDateTime_FromDateTimeOffset_ReturnsUtcDateTime()
    {
        // Arrange
        var dto = new DateTimeOffset(2024, 1, 15, 8, 0, 0, TimeSpan.FromHours(5));

        // Act
        var result = ((object)dto).ConvertToDateTime();

        // Assert
        Assert.That(result, Is.EqualTo(dto.UtcDateTime));
    }

    [Test]
    public void ConvertToDateTime_FromValidString_ParsesCorrectly()
    {
        // Act
        var result = ((object)"2024-01-15").ConvertToDateTime();

        // Assert
        Assert.That(result.Year, Is.EqualTo(2024));
        Assert.That(result.Month, Is.EqualTo(1));
        Assert.That(result.Day, Is.EqualTo(15));
    }

    [Test]
    public void ConvertToDateTime_FromInvalidValue_ReturnsMinValue()
    {
        // Act
        var result = ((object)"invalid").ConvertToDateTime();

        // Assert
        Assert.That(result, Is.EqualTo(DateTime.MinValue));
    }

    #endregion

    #region ConvertToNullableDateTimeOffset Tests

    [Test]
    public void ConvertToNullableDateTimeOffset_Null_ReturnsNull()
    {
        // Act
        var result = ((object?)null).ConvertToNullableDateTimeOffset();

        // Assert
        Assert.That(result, Is.Null);
    }

    [Test]
    public void ConvertToNullableDateTimeOffset_ValidValue_ReturnsValue()
    {
        // Arrange
        var dto = new DateTimeOffset(2024, 3, 1, 0, 0, 0, TimeSpan.Zero);

        // Act
        var result = ((object)dto).ConvertToNullableDateTimeOffset();

        // Assert
        Assert.That(result, Is.EqualTo(dto));
    }

    [Test]
    public void ConvertToNullableDateTimeOffset_InvalidValue_ReturnsNull()
    {
        // Act
        var result = ((object)"garbage").ConvertToNullableDateTimeOffset();

        // Assert
        Assert.That(result, Is.Null);
    }

    #endregion

    #region ConvertToNullableDateTime Tests

    [Test]
    public void ConvertToNullableDateTime_Null_ReturnsNull()
    {
        // Act
        var result = ((object?)null).ConvertToNullableDateTime();

        // Assert
        Assert.That(result, Is.Null);
    }

    [Test]
    public void ConvertToNullableDateTime_InvalidValue_ReturnsNull()
    {
        // Act
        var result = ((object)"nope").ConvertToNullableDateTime();

        // Assert
        Assert.That(result, Is.Null);
    }

    #endregion

    #region ConvertToStringList Tests

    [Test]
    public void ConvertToStringList_FromListOfStrings_ReturnsSameList()
    {
        // Arrange
        var input = new List<string> { "a", "b", "c" };

        // Act
        var result = ((object)input).ConvertToStringList();

        // Assert
        Assert.That(result, Is.EqualTo(input));
    }

    [Test]
    public void ConvertToStringList_FromSingleString_ReturnsListWithOneElement()
    {
        // Act
        var result = ((object)"single").ConvertToStringList();

        // Assert
        Assert.That(result, Has.Count.EqualTo(1));
        Assert.That(result[0], Is.EqualTo("single"));
    }

    [Test]
    public void ConvertToStringList_FromObjectEnumerable_ConvertsToStrings()
    {
        // Arrange
        var input = new List<object> { "x", "y", "z" };

        // Act
        var result = ((object)input).ConvertToStringList();

        // Assert
        Assert.That(result, Is.EqualTo(new List<string> { "x", "y", "z" }));
    }

    [Test]
    public void ConvertToStringList_UnsupportedType_ReturnsEmptyList()
    {
        // Act
        var result = ((object)42).ConvertToStringList();

        // Assert
        Assert.That(result, Is.Empty);
    }

    #endregion

    #region ConvertToInt Tests

    [Test]
    public void ConvertToInt_FromInt_ReturnsSameValue()
    {
        // Act
        var result = ((object)42).ConvertToInt();

        // Assert
        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public void ConvertToInt_FromLong_ConvertsToInt()
    {
        // Act
        var result = ((object)100L).ConvertToInt();

        // Assert
        Assert.That(result, Is.EqualTo(100));
    }

    [Test]
    public void ConvertToInt_FromString_ReturnsZero()
    {
        // Act
        var result = ((object)"not-a-number").ConvertToInt();

        // Assert
        Assert.That(result, Is.EqualTo(0));
    }

    #endregion
}
