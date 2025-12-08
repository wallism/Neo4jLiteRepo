using Microsoft.Extensions.Logging;

namespace Neo4jLiteRepo.Helpers
{
    /// <summary>
    /// Provides methods for generating deterministic IDs based on text content.
    /// </summary>
    public static class IdGenerator
    {
        private static readonly ILogger _logger = LoggingProvider.CreateLogger(typeof(IdGenerator).FullName);

        /// <summary>
        /// Generates a deterministic ID from the provided text.
        /// </summary>
        /// <param name="fromText">The text to generate the ID from.</param>
        /// <param name="prefix">Optional prefix to add to the generated ID.</param>
        /// <param name="maxLength">Maximum length of the generated ID.</param>
        /// <returns>A deterministic ID based on the input text or a GUID if text is invalid.</returns>
        public static string GenerateDeterministicId(string fromText, string prefix = "", int maxLength = 128)
        {
            if (string.IsNullOrWhiteSpace(fromText))
            {
                _logger.LogWarning("Empty text provided for ID generation, using GUID with prefix '{Prefix}'", prefix);
                return $"{prefix}{GetNewGuid()}";
            }
            fromText = fromText.Trim();

            // Slugify the title
            var slug = ExtensionMethods.Slugify(fromText);

            // Fallback
            if (string.IsNullOrWhiteSpace(slug))
            {
                _logger.LogWarning("Slugify resulted in empty string, using GUID with prefix '{Prefix}'", prefix);
                return $"{prefix}{GetNewGuid()}";
            }

            slug = $"{prefix}{slug}";

            // Enforce preferred length
            if (slug.Length > maxLength)
            {
                slug = slug.Substring(0, maxLength);

                // Ensure last char is alphanumeric
                while (slug.Length > 0 && !char.IsLetterOrDigit(slug[^1]))
                    slug = slug[..^1];

                if (slug.Length == 0)
                {
                    _logger.LogWarning("After trimming, slug became empty, using GUID with prefix '{Prefix}'", prefix);
                    return $"{prefix}{GetNewGuid()}";
                }
            }

            return slug;
        }

        private static Guid GetNewGuid()
        {
            var guid = Guid.NewGuid();
            _logger.LogWarning("Generated new GUID for an ID");
            return guid;
        }
    }
}
