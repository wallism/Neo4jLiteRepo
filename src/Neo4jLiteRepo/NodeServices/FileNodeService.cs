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
            // if there is a parent, data will be loaded from the parent.
            // We still need to load the data via RefreshNodeData (which should return from DataSourceService.allNodes)
            if (!string.IsNullOrWhiteSpace(ParentDataSource))
                return await RefreshNodeData();

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

            var data = await LoadDataFromFile(fullFilePath);
            return data ?? [];
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
            throw;
        }
    }

    protected async Task<IList<GraphNode>?> LoadDataFromFile(string fullFilePath)
    {
        var fileName = Path.GetFileName(fullFilePath);

        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"read data from file {fileName}");
        Console.ResetColor();

        if (!File.Exists(fullFilePath))
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"file {fileName} does not exist");
            Console.ResetColor();
            return [];
        }

        var json = await File.ReadAllTextAsync(fullFilePath);
        var data = JsonConvert.DeserializeObject<IList<GraphNode>>(json, new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Auto // Ensures polymorphic deserialization
        });
        return data;
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

            var filePath = GetFullFilePath(fileName);

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