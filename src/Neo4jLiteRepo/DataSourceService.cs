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

    Dictionary<string, IEnumerable<GraphNode>> GetAllSourceNodes();

    IEnumerable<T> GetSourceNodesFor<T>() where T : GraphNode;
    IEnumerable<T> GetSourceNodesFor<T>(string key) where T : GraphNode;

    T? GetSourceNodeFor<T>(string nodePrimaryKeyValue) where T : GraphNode;
    T? GetSourceNodeFor<T>(string key, string nodePrimaryKeyValue) where T : GraphNode;

    void AddSourceNodes<T>(List<T> nodes) where T : GraphNode;
    void AddSourceNodes<T>(string key, List<T> nodes) where T : GraphNode;
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


    public IEnumerable<T> GetSourceNodesFor<T>() where T : GraphNode
    {
        var key = typeof(T).Name;
        return GetSourceNodesFor<T>(key);
    }

    /// <summary>
    /// Get Source Nodes of a specific type. "key" is the type of T, e.g. "WebApp". This overload would be redundant,
    /// it is here to allow a call using reflection (e.g. in Neo4jGenericRepo)
    /// </summary>
    public IEnumerable<T> GetSourceNodesFor<T>(string key) where T : GraphNode
    {
        if (allNodes.TryGetValue(key, out var sourceNodesFor))
        {
            return sourceNodesFor.OfType<T>();
        }

        logger.LogInformation("{key} currently not in allNodes", key);
        return [];
    }

    public T? GetSourceNodeFor<T>(string nodePrimaryKeyValue) where T : GraphNode
    {
        var key = typeof(T).Name;
        return GetSourceNodeFor<T>(key, nodePrimaryKeyValue);
    }

    /// <summary>
    /// Get a specific Node. "key" is the type of T, e.g. "WebApp". This overload would be redundant but it is here
    /// to allow a call using reflection (e.g. in Neo4jGenericRepo
    /// </summary>
    public T? GetSourceNodeFor<T>(string key, string nodePrimaryKeyValue) where T : GraphNode
    {
        var sourceNodes = GetSourceNodesFor<T>(key);
        // null warning suppressed because GetPrimaryKeyValue throws an ex if the value is null
        return sourceNodes.FirstOrDefault(n => n.GetPrimaryKeyValue()!.Equals(nodePrimaryKeyValue, StringComparison.InvariantCultureIgnoreCase));
    }

    /// <summary>
    /// Add nodes to any existing nodes for the given key.
    /// For edge cases where you need to add nodes to the data source. Normally this is done by the data loaders LoadData.
    /// </summary>
    public void AddSourceNodes<T>(List<T> nodes) where T : GraphNode
    {
        var key = typeof(T).Name;
        AddSourceNodes(key, nodes);
    }

    /// <summary>
    /// Add nodes to any existing nodes for the given key.
    /// </summary>
    /// <remarks>won't add duplicates (matches on Id)</remarks>
    public void AddSourceNodes<T>(string key, List<T> nodes) where T : GraphNode
    {
        if (!nodes.Any())
            return;

        allNodes.TryGetValue(key, out var allExisting);
        if (allExisting == null)
        {
            allNodes[key] = nodes;
        }
        else
        {
            var existing = allExisting.ToList();
            nodes.ForEach(n => {
                var nPk = n.GetPrimaryKeyValue();
                if (nPk == null)
                    return;
                if (!existing.Any(e => e.GetPrimaryKeyValue() == nPk))
                    existing.Add(n);
            });
            allNodes[key] = existing;
        }
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