using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Neo4jLiteRepo.Helpers
{
    public interface IDataRefreshPolicy
    {
        bool AlwaysLoadFromFile { get; }
        bool ShouldRefreshNode(string nodeName);
        bool ShouldSkipNodeType(string nodeType);
        bool UpdateAllArticles { get; }
    }

    public class DataRefreshPolicy : IDataRefreshPolicy
    {
        private readonly ILogger<DataRefreshPolicy> logger;
        private readonly DataRefreshPolicySettings dataRefreshPolicySettings;

        public DataRefreshPolicy(IConfiguration config, ILogger<DataRefreshPolicy> logger)
        {
            this.logger = logger;
            var section = config.GetSection("Neo4jLiteRepo:DataRefreshPolicy");
            dataRefreshPolicySettings = section.Get<DataRefreshPolicySettings>() ?? new DataRefreshPolicySettings();
        }

        public bool ShouldRefreshAll()
            => dataRefreshPolicySettings.ForceRefresh.Contains("All");

        public bool AlwaysLoadFromFile
            => dataRefreshPolicySettings.AlwaysLoadFromFile;

        /// <summary>
        /// Update all articles, regardless of LastUpdated.
        /// </summary>
        public bool UpdateAllArticles
            => dataRefreshPolicySettings.UpdateAllArticles;

        public bool ShouldRefreshNode(string nodeName)
        {
            if (ShouldRefreshAll())
                return true;

            foreach (var configuredNode in dataRefreshPolicySettings.ForceRefresh)
            {
                // may have a prefix of "!" to indicate not to refresh
                if (!configuredNode.Contains(nodeName, StringComparison.InvariantCultureIgnoreCase))
                    continue;

                return !configuredNode.StartsWith("!");
            }

            return false;
        }

        public bool ShouldSkipNodeType(string nodeType)
        {
            if (dataRefreshPolicySettings.SkipNodeTypes.Contains(nodeType))
            {
                logger.LogWarning("Skipping {Label} nodes", nodeType);
                return true;
            }
            return false;
        }

    }

    public class DataRefreshPolicySettings
    {
        public List<string> ForceRefresh { get; set; } = [];
        public List<string> SkipNodeTypes { get; set; } = [];
        public bool AlwaysLoadFromFile { get; set; }
        public bool UpdateAllArticles { get; set; } = false;
    }
}