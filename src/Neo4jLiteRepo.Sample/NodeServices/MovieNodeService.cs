using Microsoft.Extensions.Configuration;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.Models;
using Neo4jLiteRepo.NodeServices;
using Neo4jLiteRepo.Sample.Edges;
using Neo4jLiteRepo.Sample.Nodes;

namespace Neo4jLiteRepo.Sample.NodeServices
{
    public class MovieNodeService(IConfiguration config, IDataRefreshPolicy dataRefreshPolicy, IDataSourceService dataSourceService)
        : FileNodeService<Movie>(config, dataRefreshPolicy)
    {
        private readonly IDataSourceService _dataSourceService = dataSourceService;

        public override async Task<IEnumerable<GraphNode>> LoadData(string? fileName = null)
        {
            return await RefreshNodeData();
        }

        public override async Task<IEnumerable<GraphNode>> LoadDataFromSource()
        {
            var fullFilePath = DataLoadHelpers.GetFullFilePath<Movie>(SourceFilesRootPath);
            // source for sample data is simply the json files.
            return await LoadDataFromFile(fullFilePath);
        }

#pragma warning disable CS0618 // Suppress obsolete warning for MovieGenreEdgeSeed
        public override async Task<bool> RefreshNodeRelationships(IEnumerable<GraphNode> data)
        {
            var movies = data.OfType<Movie>().ToList();
            if (movies.Count == 0)
                return true;

            // Load EdgeSeed data for Movie -> Genre relationships
            // this makes the EdgeSeeds available in the DataSourceService
            var success = await _dataSourceService.LoadEdgeSeedsFromFileAsync<MovieGenreEdge>(SourceFilesRootPath);
            if (!success)
            {
                Console.WriteLine("Failed to load MovieGenreEdgeSeed data.");
                return false;
            }

            return true;
        }
#pragma warning restore CS0618
    }
}
