using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.Models; // added for EdgeSeed
using Neo4jLiteRepo.NodeServices;
using Newtonsoft.Json;

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

    // ---------------- EdgeSeed support ----------------

    /// <summary>
    /// Get all edge sources currently loaded (by key).
    /// </summary>
    Dictionary<string, IEnumerable<EdgeSeed>> GetAllSourceEdges();

    /// <summary>
    /// Get EdgeSeed items for a specific type T (key = typeof(T).Name).
    /// </summary>
    IEnumerable<T> GetSourceEdgesFor<T>() where T : EdgeSeed;

    /// <summary>
    /// Get EdgeSeed items for a specific key.
    /// </summary>
    IEnumerable<T> GetSourceEdgesFor<T>(string key) where T : EdgeSeed;

    /// <summary>
    /// Add edges to any existing edges for the given type T (key = typeof(T).Name).
    /// </summary>
    void AddSourceEdges<T>(List<T> edges) where T : EdgeSeed;

    /// <summary>
    /// Add edges to any existing edges for the given key.
    /// </summary>
    void AddSourceEdges<T>(string key, List<T> edges) where T : EdgeSeed;

    Task<bool> LoadEdgeSeedsFromFileAsync<T>(string sourceFilesRootPath) where T : EdgeSeed;
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

    // New: in-memory store for edges
    private Dictionary<string, IEnumerable<EdgeSeed>> allEdges = [];

    public Dictionary<string, IEnumerable<GraphNode>> GetAllSourceNodes()
        => allNodes;

    // New: return all edges
    public Dictionary<string, IEnumerable<EdgeSeed>> GetAllSourceEdges()
        => allEdges;

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

    // New: Get edges for a type (key = typeof(T).Name)
    public IEnumerable<T> GetSourceEdgesFor<T>() where T : EdgeSeed
    {
        var key = typeof(T).Name;
        return GetSourceEdgesFor<T>(key);
    }

    // New: Get edges for a key
    public IEnumerable<T> GetSourceEdgesFor<T>(string key) where T : EdgeSeed
    {
        if (allEdges.TryGetValue(key, out var sourceEdgesFor))
        {
            return sourceEdgesFor.OfType<T>();
        }

        logger.LogInformation("{key} currently not in allEdges", key);
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
    public void AddSourceNodes<T>(List<T> nodes) 
        where T : GraphNode
    {
        var key = typeof(T).Name;
        AddSourceNodes(key, nodes);
    }

    /// <summary>
    /// Add nodes to any existing nodes for the given key.
    /// </summary>
    /// <remarks>won't add duplicates (matches on Id)</remarks>
    public void AddSourceNodes<T>(string key, List<T> nodes) 
        where T : GraphNode
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
                if (existing.All(e => e.GetPrimaryKeyValue() != nPk))
                    existing.Add(n);
            });
            allNodes[key] = existing;
        }
    }

    // New: Add edges with key derived from type
    public void AddSourceEdges<T>(List<T> edges) where T : EdgeSeed
    {
        var key = typeof(T).Name;
        AddSourceEdges(key, edges);
    }

    // New: Add edges to any existing edges for the given key.
    // Note: EdgeSeed has no primary key; this method appends items.
    public void AddSourceEdges<T>(string key, List<T> edges) where T : EdgeSeed
    {
        if (!edges.Any())
            return;

        allEdges.TryGetValue(key, out var allExisting);
        if (allExisting == null)
        {
            allEdges[key] = edges;
        }
        else
        {
            var existing = allExisting.ToList();
            existing.AddRange(edges);
            allEdges[key] = existing;
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

    public async Task<bool> LoadEdgeSeedsFromFileAsync<T>(string sourceFilesRootPath) where T : EdgeSeed
    {
        var fileName = typeof(T).Name + ".json";
        var fullFilePath = Path.Combine(sourceFilesRootPath, fileName);
        var json = await DataLoadHelpers.LoadJsonFromFile(fullFilePath);

        if (string.IsNullOrEmpty(json))
            return false;

        var loaded = JsonConvert.DeserializeObject<IEnumerable<EdgeSeed>>(json, new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Auto // Ensures polymorphic deserialization
        });
        var edgeSeeds = loaded?.OfType<T>().ToList();
        if (edgeSeeds == null || !edgeSeeds.Any())
            return false;

        AddSourceEdges(edgeSeeds);
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

        // New: also log edge counts when present
        if (allEdges.Count > 0)
        {
            logger.LogInformation("EdgeSeed groups to seed: {count}", allEdges.Count);
            foreach (var edgeByType in allEdges)
            {
                logger.LogInformation("{edgeType}: {count}", edgeByType.Key.PadLeft(23), edgeByType.Value.Count());
            }
        }
    }
}