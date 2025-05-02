using Microsoft.Extensions.Configuration;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.NodeServices;
using Neo4jLiteRepo.Sample.Nodes;

namespace Neo4jLiteRepo.Sample.NodeServices;

public class GenreNodeService(IConfiguration config, IDataRefreshPolicy dataRefreshPolicy)
    : FileNodeService<Genre>(config, dataRefreshPolicy)
{
    /// <summary>
    /// For the Sample, the data is static, so no need to refresh
    /// </summary>
    public override Task<IList<GraphNode>> RefreshNodeData(bool saveToFile = true) => Task.FromResult<IList<GraphNode>>([]);

    public override Task<IEnumerable<GraphNode>> LoadDataFromSource()
    {
        // source data is static json files for the sample
        throw new NotImplementedException();
    }

    public override Task<bool> RefreshNodeRelationships(IEnumerable<GraphNode> data)
    {
        throw new NotImplementedException();
    }
}