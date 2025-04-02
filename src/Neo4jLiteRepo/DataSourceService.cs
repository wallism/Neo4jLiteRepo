using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.NodeServices;

namespace Neo4jLiteRepo;

public interface IDataSourceService
{
    /// <summary>
    /// Load all node data from the data loaders into memory (allNodes)
    /// </summary>
    /// <remarks>put into memory for later use, to allow for different
    /// ways of processing the data. </remarks>
    Task<bool> LoadAllNodeDataAsync();

    IEnumerable<GraphNode> GetSourceNodesFor(Type nodeType);
    IEnumerable<GraphNode> GetSourceNodesFor(string key);
    Dictionary<string, IEnumerable<GraphNode>> GetAllSourceNodes();
    GraphNode? GetSourceNodeFor(string key, string nodePrimaryKeyValue);
    void AddSourceNodes<T>(List<T> nodes) where T : GraphNode;
}

/// <summary>
/// Load all node data from the data loaders into memory (allNodes)
/// </summary>
/// <remarks>put into memory for later use, to allow for different
/// ways of processing the data. </remarks>
public class DataSourceService(ILogger<DataSourceService> logger,
    IServiceProvider serviceProvider) : IDataSourceService
{
    private Dictionary<string, IEnumerable<GraphNode>> allNodes = [];

    public Dictionary<string, IEnumerable<GraphNode>> GetAllSourceNodes()
        => allNodes;


    public IEnumerable<GraphNode> GetSourceNodesFor(Type nodeType)
        => GetSourceNodesFor(nodeType.Name);


    public IEnumerable<GraphNode> GetSourceNodesFor(string key)
    {
        if (allNodes.TryGetValue(key, out var sourceNodesFor))
        {
            return sourceNodesFor;
        }

        logger.LogWarning("{key} not found in allNodes source data", key);
        return [];
    }

    public GraphNode? GetSourceNodeFor(string key, string nodePrimaryKeyValue)
    {
        var sourceNodes = GetSourceNodesFor(key);
        return sourceNodes.FirstOrDefault(n => n.GetPrimaryKeyValue() == nodePrimaryKeyValue);
    }

    /// <summary>
    /// Add nodes to any existing nodes for the given key.
    /// For edge cases where you need to add nodes to the data source. Normally this is done by the data loaders LoadData.
    /// </summary>
    public void AddSourceNodes<T>(List<T> nodes) where T : GraphNode
    {
        var key = typeof(T).Name;
        allNodes.TryGetValue(key, out var allExisting);
        allNodes[key] = allExisting != null
            ? allExisting.Concat(nodes).ToList()
            : nodes;
    }

    /// <summary>
    /// Load all node data from the data loaders into memory (allNodes)
    /// </summary>
    /// <remarks>put into memory for later use, to allow for different
    /// ways of processing the data. </remarks>
    public async Task<bool> LoadAllNodeDataAsync()
    {
        var loaders = serviceProvider.GetServices<INodeService>()
            .OrderBy(s => s.LoadPriority);
        foreach (var loader in loaders)
        {
            try
            {
                var nodes = await loader.LoadData().ConfigureAwait(false);

                // key could exist, ignore if it does (data added to allNodes elsewhere, probably during a data refresh).
                var addResult = allNodes.TryAdd(loader.GetNodeKeyName(), nodes);
                if (!addResult)
                    logger.LogWarning("Failed to add {nodeType} to allNodes (may already exist?)", loader.GetNodeKeyName());
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "load data");
                return false;
            }
        }

        LogDataCounts();
        return true;
    }

    private void LogDataCounts()
    {
        logger.LogInformation("GraphNode types to seed: {count}", allNodes.Count);
        // first, log the counts
        foreach (var nodeByType in allNodes)
        {
            logger.LogInformation("{nodeType}: {count}", nodeByType.Key.PadLeft(23), nodeByType.Value.Count());
        }
    }
}