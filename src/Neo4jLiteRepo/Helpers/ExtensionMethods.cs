using System.Globalization;
using System.Text;
using Neo4jLiteRepo.NodeServices;
using System.Text.RegularExpressions;

namespace Neo4jLiteRepo.Helpers
{
    public static class ExtensionMethods
    {
        /// <summary>
        /// properties have Camel casing
        /// </summary>
        internal static string ToGraphPropertyCasing(this string original)
        {
            return string.IsNullOrWhiteSpace(original)
                ? string.Empty
                : original.ToCamelCase();
        }

        /// <summary>
        /// properties have Camel casing
        /// </summary>
        internal static string ToGraphRelationShipCasing(this string original)
        {
            return original.ToUpper();
        }

        internal static string GetNodeKeyName(this INodeService nodeService)
        {
            return nodeService.GetType().Name.Replace("NodeService", string.Empty);
        }


        internal static string ExtractLastSegment(this string input, string delimiter = "/", string nullReplacement = "none")
        {
            return string.IsNullOrWhiteSpace(input)
                ? nullReplacement
                : input.Split(delimiter).Last();
        }


        public static object? SanitizeForCypher(this object? value)
        {
            return value is null ? 
                value
                : value.ToString().SanitizeForCypher();
        }
        public static object? SanitizeForCypher(this string? value)
        {
            if (value is null)
                return value;

            return value.
                Replace("\\", "\\\\")  // Escape backslashes
                .Replace("\"", "\\\""); // Escape double quotes
        }

        /// <summary>
        /// camelCaseLikeThis
        /// </summary>
        /// <param name="original"></param>
        /// <returns></returns>
        internal static string ToCamelCase(this string original)
        {
            var pascal = original.ToPascalCase();
            return string.Concat(char.ToLowerInvariant(pascal[0]), pascal[1..]);
        }

        /// <summary>
        /// PascalCaseLikeThis
        /// </summary>
        public static string ToPascalCase(this string original)
        {
            var invalidCharsRgx = new Regex("[^_a-zA-Z0-9]");
            var whiteSpace = new Regex(@"(?<=\s)");
            var startsWithLowerCaseChar = new Regex("^[a-z]");
            var firstCharFollowedByUpperCasesOnly = new Regex("(?<=[A-Z])[A-Z0-9]+$");
            var lowerCaseNextToNumber = new Regex("(?<=[0-9])[a-z]");
            var upperCaseInside = new Regex("(?<=[A-Z])[A-Z]+?((?=[A-Z][a-z])|(?=[0-9]))");

            // replace white spaces with undescore, then replace all invalid chars with empty string
            var pascalCase = invalidCharsRgx.Replace(whiteSpace.Replace(original, "_"), string.Empty)
                // split by underscores
                .Split(new[] { '_' }, StringSplitOptions.RemoveEmptyEntries)
                // set first letter to uppercase
                .Select(w => startsWithLowerCaseChar.Replace(w, m => m.Value.ToUpper()))
                // replace second and all following upper case letters to lower if there is no next lower (ABC -> Abc)
                .Select(w => firstCharFollowedByUpperCasesOnly.Replace(w, m => m.Value.ToLower()))
                // set upper case the first lower case following a number (Ab9cd -> Ab9Cd)
                .Select(w => lowerCaseNextToNumber.Replace(w, m => m.Value.ToUpper()))
                // lower second and next upper case letters except the last if it follows by any lower (ABcDEf -> AbcDef)
                .Select(w => upperCaseInside.Replace(w, m => m.Value.ToLower()));

            return string.Concat(pascalCase);
        }

        internal static bool IsBool(this object value)
        {
            return bool.TryParse(value.ToString(), out _);
        }
        /// <summary>
        /// Converts a string to a URL-friendly slug.
        /// </summary>
        public static string Slugify(this string fromText)
        {
            var slug = RemoveDiacritics(fromText.ToLowerInvariant());
            slug = Regex.Replace(slug, @"[^a-z0-9\s-]", ""); // remove special characters
            slug = Regex.Replace(slug, @"[\s-]+", " ").Trim(); // collapse whitespace
            slug = slug.Replace(" ", "-"); // replace spaces with dashes
            return slug;
        }

        private static string RemoveDiacritics(string text)
        {
            var normalized = text.Normalize(NormalizationForm.FormD);
            return new string(normalized
                    .Where(c => CharUnicodeInfo.GetUnicodeCategory(c) != UnicodeCategory.NonSpacingMark)
                    .ToArray())
                .Normalize(NormalizationForm.FormC);
        }
    }
}
