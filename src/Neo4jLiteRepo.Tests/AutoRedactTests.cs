﻿using Microsoft.Extensions.Configuration;
using Neo4jLiteRepo.Helpers;
using NUnit.Framework;

namespace Neo4jLiteRepo.Tests
{
    [TestFixture]
    public class AutoRedactTest
    {
        const string sectionKey = "Neo4jLiteRepo:AutoRedactProperties";

        [SetUp]
        public void SetUp()
        {
        }

        private void SetupConfigValues(Dictionary<string, string> inMemorySettings)
        {
            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(inMemorySettings)
                .Build();

            ConfigHelper.Initialize(configuration);
        }

        [Test]
        public void AutoRedact_ValueIsNotString_ReturnsOriginalValue()
        {
            // Arrange
            var value = 123;
            var propertyName = "SensitiveProperty";

            // Act
            var result = value.AutoRedact(propertyName);

            // Assert
            Assert.That(result, Is.EqualTo(value));
        }

        [Test]
        public void AutoRedact_PropertyNameIsNullOrEmpty_ReturnsOriginalValue()
        {
            // Arrange
            object value = "SensitiveData";
            string propertyName = null;

            // Act
            var result = value.AutoRedact(propertyName);

            // Assert
            Assert.That(result, Is.EqualTo(value));
        }

        [Test]
        public void AutoRedact_ValueIsString_NotInRedactableProperties_ReturnsOriginalValue()
        {
            // Arrange
            object value = "SensitiveData";
            var propertyName = "NonSensitiveProperty";
            SetupConfigValues(new Dictionary<string, string>());
            
            // Act
            var result = value.AutoRedact(propertyName);

            // Assert
            Assert.That(result, Is.EqualTo(value));
        }

        [Test]
        public void GetConfigValues_ShouldReturnArray_WhenSectionExists()
        {
            // Arrange
            var expectedValue = "testvalue";

            var inMemorySettings = new Dictionary<string, string> {
                { $"{sectionKey}:0", expectedValue }
            };

            SetupConfigValues(inMemorySettings);


            // Act
            var result = ConfigHelper.GetConfigValues(sectionKey);

            // Assert
            Assert.That(result, Is.EqualTo([ expectedValue ]));
        }

        [Test]
        public void AutoRedact_ValueIsString_MatchesRedactableProperty_StartSuffix_ReturnsRedacted()
        {
            // Arrange
            var autoRedactProperty = "Sensitive*";

            var inMemorySettings = new Dictionary<string, string> {
                { $"{sectionKey}:0", autoRedactProperty }
            };

            SetupConfigValues(inMemorySettings);

            var value = "SensitiveData";
            var propertyName = "SensitiveProperty";

            // Act
            var result = value.AutoRedact(propertyName);

            // Assert
            Assert.That(result, Is.EqualTo("REDACTED"));
        }

        [Test]
        public void AutoRedact_ValueIsString_MatchesRedactableProperty_StartPrefix_ReturnsRedacted()
        {
            // Arrange
            var autoRedactProperty = "*Sensitive";

            var inMemorySettings = new Dictionary<string, string> {
                { $"{sectionKey}:0", autoRedactProperty }
            };

            SetupConfigValues(inMemorySettings);

            var value = "SensitiveData";
            var propertyName = "MySensitive";

            // Act
            var result = value.AutoRedact(propertyName);

            // Assert
            Assert.That(result, Is.EqualTo("REDACTED"));
        }

        [Test]
        public void AutoRedact_ValueIsString_MatchesRedactableProperty_AllStar_ReturnsRedacted()
        {
            // Arrange
            var autoRedactProperty = "*Sensitive*";

            var inMemorySettings = new Dictionary<string, string> {
                { $"{sectionKey}:0", autoRedactProperty }
            };

            SetupConfigValues(inMemorySettings);

            var value = "SensitiveData";
            var propertyName = "MySensitiveProperty";

            // Act
            var result = value.AutoRedact(propertyName);

            // Assert
            Assert.That(result, Is.EqualTo("REDACTED"));
        }

        [Test]
        public void AutoRedact_ValueIsString_NotMatchesRedactableProperty_ReturnsValue()
        {
            // Arrange
            var autoRedactProperty = "Sensitive*";

            var inMemorySettings = new Dictionary<string, string> {
                { $"{sectionKey}:0", autoRedactProperty }
            };

            SetupConfigValues(inMemorySettings);

            var value = "SensitiveData";
            var propertyName = "MySensitiveProperty";

            // Act
            var result = value.AutoRedact(propertyName);

            // Assert
            Assert.That(result, Is.EqualTo(value));
        }

        [Test]
        public void AutoRedact_ValueIsEmptyString_ReturnsEmptyString()
        {
            // Arrange
            object value = string.Empty;
            string propertyName = "SensitiveProperty";

            // Act
            var result = value.AutoRedact(propertyName);

            // Assert
            Assert.That(result, Is.EqualTo(string.Empty));
        }
    }
}