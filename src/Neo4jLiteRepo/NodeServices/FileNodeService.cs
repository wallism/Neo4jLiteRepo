using Microsoft.Extensions.Configuration;
using Neo4jLiteRepo.Helpers;
using Newtonsoft.Json;

namespace Neo4jLiteRepo.NodeServices;

/// <summary>
/// Simplest implementation of a FileNodeService that reads data from JSON files.
/// </summary>
/// <typeparam name="T"></typeparam>
public abstract class FileNodeService<T> : INodeService
    where T : GraphNode
{
    private readonly IDataRefreshPolicy _dataRefreshPolicy;

    /// <summary>
    /// Simplest implementation of a FileNodeService that reads data from JSON files.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    protected FileNodeService(IConfiguration config, 
        IDataRefreshPolicy dataRefreshPolicy)
    {
        _dataRefreshPolicy = dataRefreshPolicy;
        Config = config;
        SourceFilesRootPath = config["Neo4jLiteRepo:JsonFilePath"] ?? Environment.CurrentDirectory;
    }

    public IConfiguration Config { get; }


    protected string SourceFilesRootPath { get; set; }

    public virtual async Task<IEnumerable<GraphNode>> LoadData(string? fileName = null)
    {
        try
        {
            // if there is a parent, data will be loaded from the parent.
            // We still need to load the data via RefreshNodeData (which should return from DataSourceService.allNodes)
            if (!string.IsNullOrWhiteSpace(ParentDataSource))
                return await RefreshNodeData();

            // refresh file data if needed
            var fullFilePath = DataLoadHelpers.GetFullFilePath<T>(SourceFilesRootPath, fileName);
            if (!_dataRefreshPolicy.AlwaysLoadFromFile
                && (!File.Exists(fullFilePath)
                    || new FileInfo(fullFilePath).Length < 128
                    || _dataRefreshPolicy.ShouldRefreshNode(typeof(T).Name)))
            {
                var result = await RefreshNodeData();
                if (UseRefreshDataOnLoadData) // don't reload from the file
                    return result;
            }

            var data = await LoadDataFromFile(fullFilePath);
            return data ?? [];
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
            throw;
        }
    }

    protected async Task<IEnumerable<GraphNode>> LoadDataFromFile(string fullFilePath)
    {
        var json = await DataLoadHelpers.LoadJsonFromFile(fullFilePath);
        if (string.IsNullOrWhiteSpace(json))
            return [];
        var data = JsonConvert.DeserializeObject<IEnumerable<GraphNode>>(json, new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Auto // Ensures polymorphic deserialization
        });
        return data ?? [];
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
    /// <summary>
    /// If the data for this node is loaded when another (parent) node is loaded.
    /// Set to the name of the parent node type. 20250505 mw parent name is not currently used.
    /// </summary>
    public virtual string ParentDataSource { get; set; } = string.Empty;

    public virtual int LoadPriority => 99;

    /// <summary>
    /// When this is true, if the data is loaded (refreshed) from the source, it will not be reloaded from the file.
    /// </summary>
    public virtual bool UseRefreshDataOnLoadData => false;


    protected virtual async Task SaveToFileAsync(IEnumerable<GraphNode> data, string? fileName = null)
    {
        try
        {
            var json = JsonConvert.SerializeObject(data, Formatting.Indented, new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Auto, // Enables polymorphic serialization
                ContractResolver = new ExcludeTypeGenerationContractResolver()
            });

            var filePath = DataLoadHelpers.GetFullFilePath<T>(SourceFilesRootPath, fileName);

            // If the full file path is longer than 260 characters, trim the filename while preserving the extension
            const int maxPathLength = 260;
            if (filePath.Length > maxPathLength)
            {
                var directory = Path.GetDirectoryName(filePath) ?? string.Empty;
                var originalFileName = Path.GetFileNameWithoutExtension(filePath);
                var extension = Path.GetExtension(filePath);

                // Calculate max allowed length for the filename
                var maxFileNameLength = maxPathLength - directory.Length - extension.Length - 1; // -1 for path separator

                if (maxFileNameLength > 0 && originalFileName.Length > maxFileNameLength)
                {
                    var trimmedFileName = originalFileName[..maxFileNameLength];
                    filePath = Path.Combine(directory, trimmedFileName + extension);
                }
            }

            // Ensure the directory exists before writing the file
            var directoryPath = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(directoryPath) && !Directory.Exists(directoryPath))
            {
                Directory.CreateDirectory(directoryPath);
            }

            await File.WriteAllTextAsync(filePath, json);
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(ex);
            Console.ResetColor();
            throw;
        }
    }
}