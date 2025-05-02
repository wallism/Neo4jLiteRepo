using Microsoft.Extensions.Configuration;
using Neo4jLiteRepo.Helpers;
using Newtonsoft.Json;

namespace Neo4jLiteRepo.NodeServices;

/// <summary>
/// Simplest implementation of a FileNodeService that reads data from JSON files.
/// </summary>
/// <typeparam name="T"></typeparam>
public abstract class FileNodeService<T> : INodeService where T : GraphNode
{
    public IConfiguration Config { get; }
    private readonly IDataRefreshPolicy dataRefreshPolicy;

    protected FileNodeService(IConfiguration config, IDataRefreshPolicy dataRefreshPolicy)
    {
        this.dataRefreshPolicy = dataRefreshPolicy;
        Config = config;
        var sourceFilesRootPath = config["Neo4jLiteRepo:JsonFilePath"] ?? Environment.CurrentDirectory;
        FilePath = $"{Path.Combine(sourceFilesRootPath, typeof(T).Name)}.json";
    }

    protected string FilePath { get; set; }

    public virtual async Task<IEnumerable<GraphNode>> LoadData()
    {
        try
        {
            // refresh file data if needed
            if (!dataRefreshPolicy.AlwaysLoadFromFile
                && (!File.Exists(FilePath)
                || new FileInfo(FilePath).Length < 128
                || dataRefreshPolicy.ShouldRefreshNode(typeof(T).Name)))
            {
                var result = await RefreshNodeData();
                if (UseRefreshDataOnLoadData) // don't reload from the file
                    return result;
            }

            // load data from file
            var json = await File.ReadAllTextAsync(FilePath);
            var data = JsonConvert.DeserializeObject<IList<GraphNode>>(json, new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Auto // Ensures polymorphic deserialization
            });

            return data ?? [];
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
            throw;
        }
    }

    /// <summary>
    /// This method is called (in LoadData) to refresh the data from the source.
    /// </summary>
    /// <remarks>make private?</remarks>
    public virtual async Task<IList<GraphNode>> RefreshNodeData(bool saveToFile = true)
    {
        var data = await LoadDataFromSource().ConfigureAwait(false);
        var list = data.ToList();
        await RefreshNodeRelationships(list).ConfigureAwait(false);

        if (saveToFile)// save this data to a file
            await SaveDataToFileAsync(list).ConfigureAwait(false);
        return list;
    }

    public abstract Task<IEnumerable<GraphNode>> LoadDataFromSource();

    public abstract Task<bool> RefreshNodeRelationships(IEnumerable<GraphNode> data);

    public virtual bool EnforceUniqueConstraint { get; set; } = true;

    public virtual int LoadPriority => 99;

    /// <summary>
    /// When this is true, if the data is loaded (refreshed) from the source, it will not be reloaded from the file.
    /// </summary>
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