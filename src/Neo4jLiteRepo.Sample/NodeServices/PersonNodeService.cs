using Neo4jLiteRepo.Sample.Nodes;
using Neo4jLiteRepo.NodeServices;
using Microsoft.Extensions.Configuration;
using Neo4jLiteRepo.Helpers;

namespace Neo4jLiteRepo.Sample.NodeServices
{
    public class PersonNodeService(IConfiguration config, IDataRefreshPolicy dataRefreshPolicy) 
        : FileNodeService<Person>(config, dataRefreshPolicy)
    {
        public override async Task<bool> RefreshNodeRelationships(IEnumerable<GraphNode> nodes)
        {
            // Implement logic to refresh relationships for Person nodes
            return await Task.FromResult(true);
        }

        public override async Task<IEnumerable<GraphNode>> LoadDataFromSource()
        {
            // Implement logic to load Person data from the source
            return await Task.FromResult(new List<GraphNode>());
        }
    }
}
