using Neo4jLiteRepo.NodeServices;
using System.Text.RegularExpressions;

namespace Neo4jLiteRepo.Helpers
{
    public static class ExtensionMethods
    {
        /// <summary>
        /// properties have Camel casing
        /// </summary>
        public static string ToGraphPropertyCasing(this string original)
        {
            return original.ToCamelCase();
        }

        /// <summary>
        /// properties have Camel casing
        /// </summary>
        public static string ToGraphRelationShipCasing(this string original)
        {
            return original.ToUpper();
        }

        public static string GetNodeKeyName(this INodeService nodeService)
        {
            return nodeService.GetType().Name.Replace("NodeService", string.Empty);
        }


        public static string ExtractLastSegment(this string input, string delimiter = "/", string nullReplacement = "none")
        {
            return string.IsNullOrWhiteSpace(input)
                ? nullReplacement
                : input.Split(delimiter).Last();
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
        internal static string ToPascalCase(this string original)
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

    }
}
