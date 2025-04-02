using Microsoft.Extensions.Configuration;
using Neo4jLiteRepo.Attributes;
using Newtonsoft.Json;

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
        var data = JsonConvert.DeserializeObject<IList<GraphNode>>(json, new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Auto // Ensures polymorphic deserialization
        });

        return data ?? [];
    }

    public abstract Task<IList<GraphNode>> RefreshNodeData(bool saveToFile = true);

    public abstract Task<IEnumerable<GraphNode>> LoadDataFromSource();

    public abstract Task<bool> RefreshNodeRelationships(IEnumerable<GraphNode> data);

    public virtual bool EnforceUniqueConstraint { get; set; } = true;

    public virtual int LoadPriority => 99;
    public virtual bool UseRefreshDataOnLoadData => false;


    protected async Task SaveDataToFileAsync(IEnumerable<GraphNode> data)
    {
        var json = JsonConvert.SerializeObject(data, Formatting.Indented, new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Auto, // Enables polymorphic serialization
            ContractResolver = new ExcludeTypeGenerationContractResolver()
        });
        await File.WriteAllTextAsync(FilePath, json);
    }


}