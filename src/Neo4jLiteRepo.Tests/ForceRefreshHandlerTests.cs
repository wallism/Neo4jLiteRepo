using Microsoft.Extensions.Configuration;
using Neo4jLiteRepo.Helpers;
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
                { "Neo4jLiteRepo:ForceRefresh:0", "Node1" },
                { "Neo4jLiteRepo:ForceRefresh:1", "Node2" },
                { "Neo4jLiteRepo:ForceRefresh:2", "!NodeExcluded" }
            };

            _configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(inMemorySettings)
                .Build();
        }

        [Test]
        public void Constructor_ShouldInitializeForceRefreshList()
        {
            // Arrange & Act
            var handler = new ForceRefreshHandler(_configuration);

            // Assert
            Assert.That(handler.ShouldRefreshNode("Node1"), Is.True);
            Assert.That(handler.ShouldRefreshNode("Node2"), Is.True);
        }

        [Test]
        public void ShouldRefreshNode_ShouldReturnTrue_WhenNodeIsInForceRefreshList()
        {
            // Arrange
            var handler = new ForceRefreshHandler(_configuration);

            // Act
            var result = handler.ShouldRefreshNode("Node1");

            // Assert
            Assert.That(result, Is.True);
        }

        [Test]
        public void ShouldRefreshNode_ShouldReturnFalse_WhenNodeIsNotInForceRefreshList()
        {
            // Arrange
            var handler = new ForceRefreshHandler(_configuration);

            // Act
            var result = handler.ShouldRefreshNode("Node3");

            // Assert
            Assert.That(result, Is.False);
        }

        [Test]
        public void ShouldRefreshNode_ShouldReturnFalse_WhenNodeExcludedWithExclamationMark()
        {
            // Arrange
            var handler = new ForceRefreshHandler(_configuration);

            // Act
            var result = handler.ShouldRefreshNode("NodeExcluded");

            // Assert
            Assert.That(result, Is.False);
        }

        [Test]
        public void ShouldRefreshNode_ShouldReturnTrue_WhenForceRefreshContainsAll()
        {
            // Arrange
            var inMemorySettings = new Dictionary<string, string>
            {
                { "Neo4jLiteRepo:ForceRefresh:0", "All" }
            };

            _configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(inMemorySettings)
                .Build();

            var handler = new ForceRefreshHandler(_configuration);

            // Act
            var result = handler.ShouldRefreshNode("AnyNode");

            // Assert
            Assert.That(result, Is.True);
        }

        [Test]
        public void ShouldRefreshNode_ShouldReturnFalse_WhenForceRefreshListIsEmpty()
        {
            // Arrange
            var emptyConfig = new ConfigurationBuilder().Build();
            var handler = new ForceRefreshHandler(emptyConfig);

            // Act
            var result = handler.ShouldRefreshNode("Node1");

            // Assert
            Assert.That(result, Is.False);
        }
    }
}
