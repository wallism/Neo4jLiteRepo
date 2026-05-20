using Neo4j.Driver;
using NUnit.Framework;

namespace Neo4jLiteRepo.UnitTests;

/// <summary>
/// Unit tests for Neo4jAvailabilityFailureDetector - exception classification for connectivity issues.
/// </summary>
[TestFixture]
public class Neo4jAvailabilityTests
{
    #region IsUnavailableException Tests

    [Test]
    public void IsUnavailableException_ServiceUnavailableException_ReturnsTrue()
    {
        // Arrange
        var ex = new ServiceUnavailableException("Server not available");

        // Act
        var result = Neo4jAvailabilityFailureDetector.IsUnavailableException(ex);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public void IsUnavailableException_SessionExpiredException_ReturnsTrue()
    {
        // Arrange
        var ex = new SessionExpiredException("Session expired");

        // Act
        var result = Neo4jAvailabilityFailureDetector.IsUnavailableException(ex);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public void IsUnavailableException_ClientExceptionWithConnectivityMessage_ReturnsTrue()
    {
        // Arrange
        var ex = new ClientException("failed to connect to the server");

        // Act
        var result = Neo4jAvailabilityFailureDetector.IsUnavailableException(ex);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public void IsUnavailableException_GenericExceptionWithConnectivityMessage_ReturnsTrue()
    {
        // Arrange
        var ex = new Exception("could not create connection to the database");

        // Act
        var result = Neo4jAvailabilityFailureDetector.IsUnavailableException(ex);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public void IsUnavailableException_NestedInnerException_ReturnsTrue()
    {
        // Arrange
        var inner = new ServiceUnavailableException("inner failure");
        var outer = new Exception("Wrapper", inner);

        // Act
        var result = Neo4jAvailabilityFailureDetector.IsUnavailableException(outer);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public void IsUnavailableException_UnrelatedGenericException_ReturnsFalse()
    {
        // Arrange
        var ex = new InvalidOperationException("Something unrelated");

        // Act
        var result = Neo4jAvailabilityFailureDetector.IsUnavailableException(ex);

        // Assert
        Assert.That(result, Is.False);
    }

    [Test]
    public void IsUnavailableException_NoRoutingServersAvailable_ReturnsTrue()
    {
        // Arrange
        var ex = new Exception("no routing servers available for the cluster");

        // Act
        var result = Neo4jAvailabilityFailureDetector.IsUnavailableException(ex);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public void IsUnavailableException_FailedToConnectToWriteServer_ReturnsTrue()
    {
        // Arrange
        var ex = new Exception("failed to connect to any write server in the cluster");

        // Act
        var result = Neo4jAvailabilityFailureDetector.IsUnavailableException(ex);

        // Assert
        Assert.That(result, Is.True);
    }

    #endregion

    #region IsConnectionPoolAcquisitionTimeout Tests

    [Test]
    public void IsConnectionPoolAcquisitionTimeout_WithPoolMessage_ReturnsTrue()
    {
        // Arrange
        var ex = new ClientException("failed to obtain a connection from pool within timeout");

        // Act
        var result = Neo4jAvailabilityFailureDetector.IsConnectionPoolAcquisitionTimeout(ex);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public void IsConnectionPoolAcquisitionTimeout_GenericExceptionWithPoolMessage_ReturnsTrue()
    {
        // Arrange
        var ex = new Exception("failed to obtain a connection from pool");

        // Act
        var result = Neo4jAvailabilityFailureDetector.IsConnectionPoolAcquisitionTimeout(ex);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public void IsConnectionPoolAcquisitionTimeout_NestedInnerException_ReturnsTrue()
    {
        // Arrange
        var inner = new ClientException("failed to obtain a connection from pool");
        var outer = new Exception("Outer", inner);

        // Act
        var result = Neo4jAvailabilityFailureDetector.IsConnectionPoolAcquisitionTimeout(outer);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public void IsConnectionPoolAcquisitionTimeout_UnrelatedMessage_ReturnsFalse()
    {
        // Arrange
        var ex = new ClientException("Some other client error");

        // Act
        var result = Neo4jAvailabilityFailureDetector.IsConnectionPoolAcquisitionTimeout(ex);

        // Assert
        Assert.That(result, Is.False);
    }

    [Test]
    public void IsConnectionPoolAcquisitionTimeout_NullMessage_ReturnsFalse()
    {
        // Arrange
        var ex = new Exception();

        // Act
        var result = Neo4jAvailabilityFailureDetector.IsConnectionPoolAcquisitionTimeout(ex);

        // Assert
        Assert.That(result, Is.False);
    }

    #endregion
}
