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
            return ShouldRefreshAll() || _forceRefresh.Contains(nodeName);
        }
    }
}