using System.Text.Json;
using Microsoft.Extensions.Configuration;

namespace Neo4jLiteRepo.NodeServices;

/// <summary>
/// Simplest implementation of a FileNodeService that reads data from JSON files.
/// </summary>
/// <typeparam name="T"></typeparam>
public abstract class FileNodeService<T> : INodeService where T : GraphNode
{
    protected FileNodeService(IConfiguration config)
    {
        var sourceFilesRootPath = config["Neo4jLiteRepo:JsonFilePath"] ?? Environment.CurrentDirectory;
        FilePath = $"{Path.Combine(sourceFilesRootPath, typeof(T).Name)}.json";
    }

    protected string FilePath { get; set; }

    public async Task<IEnumerable<GraphNode>> LoadData()
    {
        var json = await File.ReadAllTextAsync(FilePath);
        var data = JsonSerializer.Deserialize<IEnumerable<T>>(json);
        return data ?? [];
    }

    public abstract Task<bool> RefreshNodeData();

    public virtual bool EnforceUniqueConstraint { get; set; } = true;
}