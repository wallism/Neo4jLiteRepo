using System.Collections;
using System.Collections.Generic;
using System.Text.Json;
using System.Xml;
using Microsoft.Extensions.Configuration;

namespace Neo4jLiteRepo.NodeServices;

/// <summary>
/// Simplest implementation of a FileNodeService that reads data from JSON files.
/// </summary>
/// <typeparam name="T"></typeparam>
public abstract class FileNodeService<T> : INodeService where T : GraphNode
{
    public IConfiguration Config { get; }

    protected FileNodeService(IConfiguration config)
    {
        Config = config;
        var sourceFilesRootPath = config["Neo4jLiteRepo:JsonFilePath"] ?? Environment.CurrentDirectory;
        FilePath = $"{Path.Combine(sourceFilesRootPath, typeof(T).Name)}.json";
    }

    protected string FilePath { get; set; }

    public virtual async Task<IEnumerable<GraphNode>> LoadData()
    {
        var json = await File.ReadAllTextAsync(FilePath);
        var data = JsonSerializer.Deserialize<IList<T>>(json);
        return data ?? [];
    }

    public abstract Task<bool> RefreshNodeData();

    public abstract Task<IEnumerable<GraphNode>> LoadDataFromSource();

    public abstract Task<bool> RefreshNodeRelationships(IEnumerable<GraphNode> data);

    public virtual bool EnforceUniqueConstraint { get; set; } = true;



    protected async Task SaveDataToFileAsync(IEnumerable<GraphNode> data)
    {
        var json = JsonSerializer.Serialize(data, new JsonSerializerOptions
        {
            MaxDepth = 20,
            WriteIndented = true
        });
        await File.WriteAllTextAsync(FilePath, json);
    }


}