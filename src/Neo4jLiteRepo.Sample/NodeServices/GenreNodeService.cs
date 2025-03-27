﻿using Microsoft.Extensions.Configuration;
using Neo4jLiteRepo.NodeServices;
using Neo4jLiteRepo.Sample.Labels;

namespace Neo4jLiteRepo.Sample.NodeServices;

public class GenreNodeService(IConfiguration config) : FileNodeService<Genre>(config)
{
    /// <summary>
    /// For the Sample, the data is static, so no need to refresh
    /// </summary>
    public override Task<bool> RefreshNodeData() => Task.FromResult(true);

}