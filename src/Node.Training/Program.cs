using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Node.Training;
using Neo4jLiteRepo.Helpers;
using Serilog;

// Configure Serilog
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.Debug()
    .CreateLogger();

Log.Information("Node Training - Starting up");

// Create host builder
var builder = Host.CreateApplicationBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddSerilog(Log.Logger);

// Configure app settings
builder.Configuration
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables();

// Register services
builder.Services.AddSingleton<NodeTrainer>();

// Build service provider
var servicesProvider = builder.Services.BuildServiceProvider();

try
{
    Log.Information("Start Training");
    
    // Get the NodeTrainer and run it
    var nodeTrainer = servicesProvider.GetRequiredService<NodeTrainer>();
    await nodeTrainer.TrainFromJsonFiles();
    
    Log.Information("Training complete");
}
catch (Exception ex)
{
    Log.Error(ex, "ex during training");
}
finally
{
    Log.CloseAndFlush();
}

//Console.WriteLine("Press any key to exit...");
//Console.ReadKey();
