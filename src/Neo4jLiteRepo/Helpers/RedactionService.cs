using Microsoft.Extensions.Configuration;
using System.Text.RegularExpressions;

namespace Neo4jLiteRepo.Helpers
{
    public interface IRedactionService
    {
        object? AutoRedact(object? value, string? propertyName);
    }

    public class RedactionService(IConfiguration configuration) : IRedactionService
    {
        public object? AutoRedact(object? value, string? propertyName)
        {
            if (value is not string || string.IsNullOrWhiteSpace(propertyName))
                return value;

            var str = value.ToString();
            if (string.IsNullOrWhiteSpace(str))
                return string.Empty;

            var autoRedactProperties = configuration.GetSection("Neo4jLiteRepo:AutoRedactProperties").Get<string[]>();
            if (autoRedactProperties is null || !autoRedactProperties.Any())
                return value;

            foreach (var pattern in autoRedactProperties)
            {
                var regexPattern = "^" + Regex.Escape(pattern).Replace("\\*", ".*") + "$";
                if (Regex.IsMatch(propertyName, regexPattern, RegexOptions.IgnoreCase))
                {
                    return "REDACTED";
                }
            }

            return value;
        }
    }
}
