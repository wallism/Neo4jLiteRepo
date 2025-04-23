using Microsoft.Extensions.Configuration;

namespace Neo4jLiteRepo.Helpers
{
    public interface IForceRefreshHandler
    {
        bool ShouldRefreshNode(string nodeName);
    }

    public class ForceRefreshHandler : IForceRefreshHandler
    {
        private readonly List<string> _forceRefresh;

        public ForceRefreshHandler(IConfiguration config)
        {
            var section = config.GetSection("Neo4jLiteRepo:ForceRefresh");
            _forceRefresh = section?.Get<List<string>>() ?? [];
        }

        private bool ShouldRefreshAll()
            => _forceRefresh.Contains("All");


        public bool ShouldRefreshNode(string nodeName)
        {
            if (ShouldRefreshAll())
                return true;

            foreach (var configuredNode in _forceRefresh)
            {
                // may have a prefix of "!" to indicate not to refresh
                if (!configuredNode.Contains(nodeName, StringComparison.InvariantCultureIgnoreCase))
                    continue;

                return !configuredNode.StartsWith("!");
            }

            return false;
        }
    }
}