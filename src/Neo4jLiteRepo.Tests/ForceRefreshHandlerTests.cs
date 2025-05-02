using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Neo4jLiteRepo.Helpers;
using NSubstitute;
using NUnit.Framework;

namespace Neo4jLiteRepo.Tests
{
    [TestFixture]
    public class ForceRefreshHandlerTests
    {
        private IConfiguration _configuration;

        [SetUp]
        public void SetUp()
        {
            var inMemorySettings = new Dictionary<string, string>
            {
                { "Neo4jLiteRepo:DataRefreshPolicy:ForceRefresh:0", "Node1" },
                { "Neo4jLiteRepo:DataRefreshPolicy:ForceRefresh:1", "Node2" },
                { "Neo4jLiteRepo:DataRefreshPolicy:ForceRefresh:2", "!NodeExcluded" }
            };

            _configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(inMemorySettings)
                .Build();
        }

        [Test]
        public void Constructor_ShouldInitializeForceRefreshList()
        {
            // Arrange & Act
            var dataRefreshPolicy = new DataRefreshPolicy(_configuration, Substitute.For<ILogger<DataRefreshPolicy>>());

            // Assert
            Assert.That(dataRefreshPolicy.ShouldRefreshNode("Node1"), Is.True);
            Assert.That(dataRefreshPolicy.ShouldRefreshNode("Node2"), Is.True);
        }

        [Test]
        public void ShouldRefreshNode_ShouldReturnTrue_WhenNodeIsInForceRefreshList()
        {
            // Arrange
            var dataRefreshPolicy = new DataRefreshPolicy(_configuration, Substitute.For<ILogger<DataRefreshPolicy>>());

            // Act
            var result = dataRefreshPolicy.ShouldRefreshNode("Node1");

            // Assert
            Assert.That(result, Is.True);
        }

        [Test]
        public void ShouldRefreshNode_ShouldReturnFalse_WhenNodeIsNotInForceRefreshList()
        {
            // Arrange
            var dataRefreshPolicy = new DataRefreshPolicy(_configuration, Substitute.For<ILogger<DataRefreshPolicy>>());

            // Act
            var result = dataRefreshPolicy.ShouldRefreshNode("Node3");

            // Assert
            Assert.That(result, Is.False);
        }

        [Test]
        public void ShouldRefreshNode_ShouldReturnFalse_WhenNodeExcludedWithExclamationMark()
        {
            // Arrange
            var dataRefreshPolicy = new DataRefreshPolicy(_configuration, Substitute.For<ILogger<DataRefreshPolicy>>());

            // Act
            var result = dataRefreshPolicy.ShouldRefreshNode("NodeExcluded");

            // Assert
            Assert.That(result, Is.False);
        }

        [Test]
        public void ShouldRefreshNode_ShouldReturnTrue_WhenForceRefreshContainsAll()
        {
            // Arrange
            var inMemorySettings = new Dictionary<string, string>
            {
                { "Neo4jLiteRepo:DataRefreshPolicy:ForceRefresh:0", "All" }
            };

            _configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(inMemorySettings)
                .Build();

            var dataRefreshPolicy = new DataRefreshPolicy(_configuration, Substitute.For<ILogger<DataRefreshPolicy>>());

            // Act
            var result = dataRefreshPolicy.ShouldRefreshNode("AnyNode");

            // Assert
            Assert.That(result, Is.True);
        }

        [Test]
        public void ShouldRefreshNode_ShouldReturnFalse_WhenForceRefreshListIsEmpty()
        {
            // Arrange
            var inMemorySettings = new Dictionary<string, string> { };

            _configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(inMemorySettings)
                .Build();
            var dataRefreshPolicy = new DataRefreshPolicy(_configuration, Substitute.For<ILogger<DataRefreshPolicy>>());

            // Act
            var result = dataRefreshPolicy.ShouldRefreshNode("Node1");

            // Assert
            Assert.That(result, Is.False);
        }
    }
}
