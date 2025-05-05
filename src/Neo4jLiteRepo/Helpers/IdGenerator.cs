using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

public static class IdGenerator
{
    private static readonly Random Random = new();

    public static string GenerateDeterministicId(string fromText, int maxLength = 50)
    {
        if (string.IsNullOrWhiteSpace(fromText))
            return $"section-{Guid.NewGuid()}";

        // Slugify the title
        var slug = RemoveDiacritics(fromText.ToLowerInvariant());
        slug = Regex.Replace(slug, @"[^a-z0-9\s-]", ""); // remove special characters
        slug = Regex.Replace(slug, @"[\s-]+", " ").Trim(); // collapse whitespace
        slug = slug.Replace(" ", "-"); // replace spaces with dashes

        // Fallback
        if (string.IsNullOrWhiteSpace(slug))
            return $"section-{Guid.NewGuid()}";

        // Enforce preferred length
        if (slug.Length > maxLength)
        {
            slug = slug.Substring(0, maxLength);

            // Ensure last char is alphanumeric
            while (slug.Length > 0 && !char.IsLetterOrDigit(slug[^1]))
                slug = slug[..^1];

            if (slug.Length == 0)
                return $"section-{Guid.NewGuid()}";
        }

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
