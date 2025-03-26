using System.Text.Json;
using Microsoft.Extensions.Configuration;

namespace Neo4jLiteRepo
{

    public interface INodeService
    {
        /// <summary>
        /// Load data from local files.
        /// </summary>
        /// <returns></returns>
        Task<IEnumerable<GraphNode>> LoadData();
        /// <summary>
        /// Refresh node data from 'your source'. 
        /// </summary>
        /// <remarks>this has been split from LoadData to expedite testing
        /// of building your graph. If your data source is an API, use
        /// this method to load that data into a local file.
        /// For production data or sensitive data, either cleanup the files
        /// immediately or change the mechanism to load the data into memory.</remarks>
        Task<bool> RefreshNodeData();

        bool EnforceUniqueConstraint { get; set; }
    }

    public abstract class FileNodeService<T> : INodeService where T : GraphNode
    {
        protected FileNodeService(IConfiguration config)
        {
            var sourceFilesRootPath = config["FileNodeService:JsonFilePath"] ?? Environment.CurrentDirectory;
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

}
