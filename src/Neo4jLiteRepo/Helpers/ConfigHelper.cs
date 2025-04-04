using Microsoft.Extensions.Configuration;

namespace Neo4jLiteRepo.Helpers;

public static class ConfigHelper
{
    private static IConfiguration? _configuration;

    public static void Initialize(IConfiguration? configuration)
    {
        _configuration = configuration;
    }

    public static string[]? GetConfigValues(string sectionKey)
    {
        return GetSection(sectionKey).Get<string[]>();
    }
    public static string? GetConfigValue(string sectionKey)
    {
        return GetSection(sectionKey).Get<string>();
    }

    private static IConfigurationSection GetSection(string sectionKey)
    {
        if (_configuration is null)
            throw new InvalidOperationException("Configuration has not been initialized. Call Initialize() first.");
        if (string.IsNullOrWhiteSpace(sectionKey))
            throw new ArgumentNullException(nameof(sectionKey), "sectionKey cannot be null or empty.");

        return _configuration.GetSection(sectionKey);
    }
}