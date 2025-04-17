using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using Neo4j.Driver;
using Neo4jLiteRepo;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.Setup;
using Neo4jLiteRepo.Sample.NodeServices;
using Neo4jLiteRepo.NodeServices;
using Newtonsoft.Json;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.Debug()
    .CreateLogger();

Log.Information("Logging setup");
// this sets up Serilog, replace with your own logger setup if needed (the Repo project does NOT have a dependency on Serilog)
var builder = Host.CreateApplicationBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddSerilog(Log.Logger);

// Configure app settings
builder.Configuration
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables();

// register the Neo4j driver
builder.Services.AddSingleton<IDriver>(_ =>
{
    var settings = new Neo4jSettings();
    builder.Configuration.GetSection("Neo4jSettings").Bind(settings);

    var driver = GraphDatabase.Driver(
        settings.Connection, AuthTokens.Basic(
    settings.User,
    settings.Password));

    Log.Information("neo4j driver initialized");
    return driver;
});

builder.Services.AddSingleton<IForceRefreshHandler, ForceRefreshHandler>();

// register node services
builder.Services.AddSingleton<INodeService, MovieNodeService>();
builder.Services.AddSingleton<INodeService, GenreNodeService>();

// register required LiteRepo services
builder.Services.AddSingleton<INeo4jGenericRepo, Neo4jGenericRepo>();
builder.Services.AddSingleton<IDataSourceService, DataSourceService>();
builder.Services.AddSingleton<IDataSeedService, DataSeedService>();

ConfigHelper.Initialize(builder.Configuration);

// in this console app, I just want to seed the data. So I just get the required services and call the seed method
var servicesProvider = builder.Services.BuildServiceProvider();
var repo = servicesProvider.GetRequiredService<INeo4jGenericRepo>();
var seedService = servicesProvider.GetRequiredService<IDataSeedService>();

try
{
    var result = await seedService.SeedAllData();

    //var result = await repo.GetNodesAndRelationshipsAsync();
    //var json = JsonConvert.SerializeObject(result);
    //Log.Logger.Information(json);
}
catch (Exception ex)
{
    Log.Error(ex, "Unhandled Ex");
}
finally
{
    Log.CloseAndFlush();
    repo.Dispose();

}


Console.WriteLine("done!");
