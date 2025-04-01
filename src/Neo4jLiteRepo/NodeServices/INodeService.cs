namespace Neo4jLiteRepo.NodeServices
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

        Task<IEnumerable<GraphNode>> LoadDataFromSource();

        Task<bool> RefreshNodeRelationships(IEnumerable<GraphNode> data);

        bool EnforceUniqueConstraint { get; set; }

        /// <summary>
        /// Higher priority services are loaded first.
        /// </summary>
        /// <remarks>Sometimes this is needed but try to avoid using it.
        /// It's better (easier) if load order doesn't matter.</remarks>
        int LoadPriority { get; }
    }
}