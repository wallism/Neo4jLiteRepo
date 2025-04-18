﻿using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Neo4jLiteRepo.NodeServices;

namespace Neo4jLiteRepo
{
    public interface IDataSeedService
    {
        Task<bool> SeedAllData();
    }


    public class DataSeedService : IDataSeedService
    {
        private readonly ILogger<DataSeedService> logger;
        private readonly IConfiguration config;
        private readonly INeo4jGenericRepo graphRepo;
        private readonly IDataSourceService dataSourceService;
        private readonly IServiceProvider serviceProvider;
        private readonly List<string> _skipNodeTypes;

        public DataSeedService(ILogger<DataSeedService> logger,
            IConfiguration config,
            INeo4jGenericRepo graphRepo,
            IDataSourceService dataSourceService,
            IServiceProvider serviceProvider)
        {
            this.logger = logger;
            this.config = config;
            this.graphRepo = graphRepo;
            this.dataSourceService = dataSourceService;
            this.serviceProvider = serviceProvider;


            var section = config.GetSection("Neo4jLiteRepo:SkipNodeTypes");
            _skipNodeTypes = section?.Get<List<string>>() ?? [];
        }

        /// <summary>
        /// Load all data and seed both Nodes and Relationships into the graph.
        /// </summary>
        public async Task<bool> SeedAllData()
        {
            var loadSourceDataResult = await dataSourceService.LoadAllNodeDataAsync();
            if (!loadSourceDataResult)
            {
                logger.LogError("Failed to load data. Exiting...");
                return false;
            }
            
            try
            {
                // it might be a fresh database or we might have new Labels, ensure unique constraints
                await EnforceUniqueConstraints().ConfigureAwait(false);
                // Why seed all nodes first? Because we need to have all nodes in the graph before we can create relationships
                await SeedAllNodes().ConfigureAwait(false);
                await SeedAllNodeRelationships().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error seeding data");
                return false;
            }
            logger.LogInformation("SeedAllData Complete!");
            return true;
        }

        private async Task<bool> EnforceUniqueConstraints()
        {
            var loaders = serviceProvider.GetServices<INodeService>();
            return await graphRepo.EnforceUniqueConstraints(loaders);
        }

        private async Task SeedAllNodes()
        {
            logger.LogInformation("Seed NODES");
            // then process the data
            foreach (var nodeByType in dataSourceService.GetAllSourceNodes())
            {
                if(ShouldSkipNodeType(nodeByType.Key))
                    continue;
                await SeedDataNodes(nodeByType.Value).ConfigureAwait(false);
            }

        }


        private async Task SeedAllNodeRelationships()
        {
            logger.LogInformation("Seed RELATIONSHIPS");
            // then process the data
            foreach (var nodeByType in dataSourceService.GetAllSourceNodes())
            {
                if (ShouldSkipNodeType(nodeByType.Key))
                    continue;
                await SeedNodeRelationships(nodeByType.Value).ConfigureAwait(false);
            }
        }


        public async Task<bool> SeedDataNodes<T>(IEnumerable<T> nodeData)
            where T : GraphNode
        {
            var graphNodes = nodeData.ToList();
            if(! graphNodes.Any())
                return false;
            
            logger.LogInformation("Seeding {Label} {Count} nodes", graphNodes.First().GetType().Name.PadLeft(20), graphNodes.Count());

            await graphRepo.UpsertNodes(graphNodes).ConfigureAwait(false);
            
            return true;
        }

        public async Task<bool> SeedNodeRelationships<T>(IEnumerable<T> nodeData)
            where T : GraphNode
        {
            var graphNodes = nodeData.ToList();
            
            await graphRepo.CreateRelationshipsAsync(graphNodes).ConfigureAwait(false);
            
            return true;
        }


        private bool ShouldSkipNodeType(string nodeType)
        {
            if (_skipNodeTypes.Contains(nodeType))
            {
                logger.LogWarning("Skipping {Label} nodes", nodeType);
                return true;
            }
            return false;
        }


        private List<Type> GetGraphNodeTypes()
        {
            return [];
            //var allTypes = AppDomain.CurrentDomain.GetAssemblies()
            //    .Where(assembly => !assembly.FullName.StartsWith("System")
            //                       && !assembly.FullName.StartsWith("Microsoft")
            //                       && !assembly.FullName.StartsWith("Neo4j.Driver")
            //                       && !assembly.FullName.StartsWith("Serilog"))
            //    .SelectMany(assembly => assembly.GetTypes())
            //    .ToList();

            //var allGraphNodeTypes = allTypes
            //    .Where(t => typeof(GraphNode).IsAssignableFrom(t)
            //                && t is { IsAbstract: false, IsInterface: false })
            //    .ToList();

            //return allGraphNodeTypes;
        }
    }
}
