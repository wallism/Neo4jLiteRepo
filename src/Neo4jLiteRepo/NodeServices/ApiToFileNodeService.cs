﻿using Microsoft.Extensions.Configuration;
using Neo4jLiteRepo.Helpers;

namespace Neo4jLiteRepo.NodeServices;

/// <summary>
/// Node data is loaded from an API and saved to a json file.
/// Particularly useful if you don't want to call the api every time (your data doesn't change often)
/// but are working on getting your graph data structure setup.
/// </summary>
/// <typeparam name="T"></typeparam>
public abstract class ApiToFileNodeService<T> : FileNodeService<T> where T : GraphNode
{
    private readonly IForceRefreshHandler _forceRefreshHandler;

    protected ApiToFileNodeService(IConfiguration config,
        IForceRefreshHandler forceRefreshHandler) : base(config)
    {
        _forceRefreshHandler = forceRefreshHandler;
        var sourceFilesRootPath = config["Neo4jLiteRepo:JsonFilePath"] ?? Environment.CurrentDirectory;
        FilePath = $"{Path.Combine(sourceFilesRootPath, typeof(T).Name)}.json";
        
    }


    protected string FilePath { get; set; }

    public override async Task<IEnumerable<GraphNode>> LoadData()
    {
        // refresh file data if needed
        if (!File.Exists(FilePath)
            || new FileInfo(FilePath).Length < 128
            || _forceRefreshHandler.ShouldRefreshNode(typeof(T).Name))
        {
            var result = await RefreshNodeData();
            if (UseRefreshDataOnLoadData) // don't reload from the file
                return result;
        }

        // load data from file
        return await base.LoadData();
    }

    public override bool UseRefreshDataOnLoadData => true;

    public abstract override Task<IEnumerable<GraphNode>> LoadDataFromSource();


    /// <summary>
    /// This method is called to refresh the data from the API.
    /// </summary>
    public override async Task<IList<GraphNode>> RefreshNodeData(bool saveToFile = true)
    {
        var data = await LoadDataFromSource().ConfigureAwait(false);
        var list = data.ToList();
        await RefreshNodeRelationships(list).ConfigureAwait(false);

        if(saveToFile)// save this data to a file
            await SaveDataToFileAsync(list).ConfigureAwait(false);
        return list;
    }

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