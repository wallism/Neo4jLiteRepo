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
    .AddUserSecrets(typeof(Program).Assembly)
    .AddEnvironmentVariables();

// register the Neo4j driver
builder.Services.AddSingleton<IDriver>(_ =>
{
    var settings = new Neo4jSettings();
    builder.Configuration.GetSection("Neo4jSettings").Bind(settings);

    var driver = GraphDatabase.Driver(
        settings.Connection, AuthTokens.Basic(
            settings.User,
            "password-for-unit-testing-db-only" // todo: revert to --> //settings.Password
    ));

    Log.Information("neo4j driver initialized");
    Task.Run(() =>
    {
        try
        {
            driver.VerifyConnectivityAsync().GetAwaiter().GetResult();
            Log.Information("neo4j connectivity verified");
        }
        catch (Exception ex)
        {
            Log.Error(ex, "neo4j connectivity FAILED. Exiting.");
            Environment.Exit(1);
        }

    }).GetAwaiter().GetResult();
    return driver;
});

builder.Services.AddSingleton<IDataRefreshPolicy, DataRefreshPolicy>();

// register node services
// Register all implementations of INodeService dynamically
var nodeServiceType = typeof(INodeService);
var assembliesToScan = new[] { typeof(MovieNodeService).Assembly };
var nodeServiceImplementations = assembliesToScan
    .SelectMany(assembly => assembly.GetTypes())
    .Where(type => nodeServiceType.IsAssignableFrom(type) && !type.IsInterface && !type.IsAbstract);

foreach (var implementation in nodeServiceImplementations)
{
    Log.Information($"Registering INodeService implementation: {implementation.Name}");
    builder.Services.AddSingleton(nodeServiceType, implementation);
}

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
