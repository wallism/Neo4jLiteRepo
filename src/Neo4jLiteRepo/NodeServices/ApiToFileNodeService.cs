using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Neo4jLiteRepo.Helpers;

namespace Neo4jLiteRepo.NodeServices;

/// <summary>
/// Node data is loaded from an API and saved to a json file.
/// Particularly useful if you don't want to call the api every time (your data doesn't change often)
/// but are working on getting your graph data structure setup.
/// </summary>
/// <typeparam name="T"></typeparam>
public abstract class ApiToFileNodeService<T> : INodeService where T : GraphNode
{
    private readonly IForceRefreshHandler _forceRefreshHandler;

    protected ApiToFileNodeService(IConfiguration config,
        IForceRefreshHandler forceRefreshHandler)
    {
        _forceRefreshHandler = forceRefreshHandler;
        var sourceFilesRootPath = config["Neo4jLiteRepo:JsonFilePath"] ?? Environment.CurrentDirectory;
        FilePath = $"{Path.Combine(sourceFilesRootPath, typeof(T).Name)}.json";
        
    }


    protected string FilePath { get; set; }

    public async Task<IEnumerable<GraphNode>> LoadData()
    {
        // refresh file data if needed
        if (!File.Exists(FilePath) || _forceRefreshHandler.ShouldRefreshNode(typeof(T).Name))
            await RefreshNodeData();

        // load data from file
        var json = await File.ReadAllTextAsync(FilePath);
        var data = JsonSerializer.Deserialize<IEnumerable<T>>(json);
        return data ?? [];
    }

    /// <summary>
    /// This method is called to refresh the data from the API.
    /// </summary>
    public abstract Task<bool> RefreshNodeData();

    public virtual bool EnforceUniqueConstraint { get; set; } = true;
}