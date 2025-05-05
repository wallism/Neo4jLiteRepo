using Microsoft.Extensions.Configuration;
using Neo4jLiteRepo.Helpers;
using Newtonsoft.Json;

namespace Neo4jLiteRepo.NodeServices;

/// <summary>
/// Simplest implementation of a FileNodeService that reads data from JSON files.
/// </summary>
/// <typeparam name="T"></typeparam>
public abstract class FileNodeService<T>(IConfiguration config,
    IDataRefreshPolicy dataRefreshPolicy) : INodeService
    where T : GraphNode
{
    public IConfiguration Config { get; } = config;

    protected string GetFullFilePath(string? fileName = null, string extension = ".json")
    {
        var path = $"{Path.Combine(SourceFilesRootPath, fileName ?? typeof(T).Name)}";
        // If the path doesn't have an extension, add .json
        if (string.IsNullOrEmpty(Path.GetExtension(path)))
        {
            path = Path.ChangeExtension(path, extension);
        }
        return path;
    }

    protected string SourceFilesRootPath { get; set; } = config["Neo4jLiteRepo:JsonFilePath"] ?? Environment.CurrentDirectory;

    public virtual async Task<IEnumerable<GraphNode>> LoadData(string? fileName = null)
    {
        try
        {
            // refresh file data if needed
            var fullFilePath = GetFullFilePath(fileName);
            if (!dataRefreshPolicy.AlwaysLoadFromFile
                && (!File.Exists(fullFilePath)
                    || new FileInfo(fullFilePath).Length < 128
                    || dataRefreshPolicy.ShouldRefreshNode(typeof(T).Name)))
            {
                var result = await RefreshNodeData();
                if (UseRefreshDataOnLoadData) // don't reload from the file
                    return result;
            }

            // load data from file
            var json = await File.ReadAllTextAsync(fullFilePath);
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
            await SaveToFileAsync(list).ConfigureAwait(false);
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


    protected async Task SaveToFileAsync(IEnumerable<GraphNode> data, string? fileName = null)
    {
        var json = JsonConvert.SerializeObject(data, Formatting.Indented, new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Auto, // Enables polymorphic serialization
            ContractResolver = new ExcludeTypeGenerationContractResolver()
        });
        await File.WriteAllTextAsync(GetFullFilePath(fileName), json);
    }


}