using Microsoft.Extensions.Configuration;
using Neo4jLiteRepo.Helpers;

namespace Neo4jLiteRepo.NodeServices;

/// <summary>
/// Node data is loaded from an API and saved to a json file.
/// Particularly useful if you don't want to call the api every time (your data doesn't change often)
/// but are working on getting your graph data structure setup.
/// </summary>
/// <typeparam name="T"></typeparam>
public abstract class ApiToFileNodeService<T>(
    IConfiguration config,
    IDataRefreshPolicy dataRefreshPolicy) : FileNodeService<T>(config, dataRefreshPolicy)
    where T : GraphNode
{
    public override bool UseRefreshDataOnLoadData => true;

    public abstract override Task<IEnumerable<GraphNode>> LoadDataFromSource();


    /// <summary>
    /// Default implementation does not build relationships.
    /// If your node has no 'outgoing' relationships, you don't need to implement.
    /// </summary>
    /// <remarks>The purpose of this function is to populate all properties decorated with the
    /// Relationship Attribute, i.e. populate the list with the string PrimaryKeys.</remarks>
    public override Task<bool> RefreshNodeRelationships(IEnumerable<GraphNode> data)
    {
        return Task.FromResult(true);
    }

}