using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Helpers;
using Neo4jLiteRepo.NodeServices;
using Neo4jLiteRepo.Sample.NodeServices;
using NUnit.Framework;
using Serilog;

namespace Neo4jLiteRepo.Tests.Integration;

/// <summary>
/// Base class for Neo4j integration tests.
/// 
/// IMPORTANT - WIPES the database during cleanup!
/// Do NOT point at an important database! (hardcoded connection is intentional to prevent accidentally wiping a production DB)
/// 
/// CONFIGURATION:
/// 1. Update Neo4jPassword in appsettings to match your Neo4j Desktop database password
/// 2. Tests use a separate database by default (requires Enterprise/AuraDB)
/// 3. If using Community Edition, override TestDatabase property to return "neo4j"
/// </summary>
public abstract class Neo4jIntegrationTestBase
{
    protected IHost Host { get; private set; } = null!;
    protected IDriver Driver { get; private set; } = null!;
    protected INeo4jGenericRepo Repo { get; private set; } = null!;

    // Hardcoded connection settings to prevent accidentally wiping production databases
    private const string Neo4jUri = "neo4j://127.0.0.1:7687";
    private const string Neo4jUser = "neo4j";

    /// <summary>
    /// Override this property to specify which database to use for tests.
    /// Default is "IntegrationTests" (requires Enterprise/AuraDB).
    /// For Community Edition, return "neo4j".
    /// </summary>
    protected virtual string TestDatabase => "IntegrationTests";

    /// <summary>
    /// Override this to customize service registration.
    /// Called during OneTimeSetup after base services are registered.
    /// </summary>
    protected virtual void RegisterAdditionalServices(HostApplicationBuilder builder)
    {
        // Base implementation does nothing - override in derived classes if needed
    }

    /// <summary>
    /// Override this to perform custom setup after services are built.
    /// Called during OneTimeSetup after the host is built.
    /// </summary>
    protected virtual Task OnPostSetupAsync()
    {
        return Task.CompletedTask;
    }

    [OneTimeSetUp]
    public async Task OneTimeSetup()
    {
        Log.Logger = new LoggerConfiguration()
            .WriteTo.Console()
            .WriteTo.Debug()
            .CreateLogger();

        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddSerilog(Log.Logger);

        builder.Configuration
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .AddJsonFile("appsettings.Development.json", optional: true, reloadOnChange: true)
            .AddUserSecrets(typeof(Neo4jIntegrationTestBase).Assembly)
            .AddEnvironmentVariables();

        // Register Neo4j driver - deliberately using hardcoded values because tests wipe the DB during cleanup
        builder.Services.AddSingleton<IDriver>(_ =>
        {
            var password = builder.Configuration["Neo4jSettings:Password"];
            var driver = GraphDatabase.Driver(Neo4jUri, AuthTokens.Basic(Neo4jUser, password));

            Log.Information("neo4j driver initialized for integration tests");
            Task.Run(() => Task.FromResult(driver.VerifyConnectivityAsync())).GetAwaiter().GetResult();
            Log.Information("neo4j connectivity verified");
            return driver;
        });

        // Configure the database name for the repository to use
        builder.Configuration["Neo4jSettings:Database"] = TestDatabase;

        builder.Services.AddSingleton<IDataRefreshPolicy, DataRefreshPolicy>();

        // Register node services dynamically
        var nodeServiceType = typeof(INodeService);
        var assembliesToScan = new[] { typeof(MovieNodeService).Assembly };
        var nodeServiceImplementations = assembliesToScan
            .SelectMany(assembly => assembly.GetTypes())
            .Where(type => nodeServiceType.IsAssignableFrom(type) && !type.IsInterface && !type.IsAbstract);

        foreach (var implementation in nodeServiceImplementations)
        {
            builder.Services.AddSingleton(nodeServiceType, implementation);
        }

        builder.Services.AddSingleton<INeo4jGenericRepo, Neo4jGenericRepo>();
        builder.Services.AddSingleton<IDataSourceService, DataSourceService>();
        builder.Services.AddSingleton<IDataSeedService, DataSeedService>();

        // Allow derived classes to register additional services
        RegisterAdditionalServices(builder);

        this.Host = builder.Build();
        Driver = Host.Services.GetRequiredService<IDriver>();
        Repo = Host.Services.GetRequiredService<INeo4jGenericRepo>();

        // Ensure test database exists 
        await EnsureDatabaseExists();
        
        // Clean up before tests to start with empty state
        await CleanupDatabase();

        // Allow derived classes to perform custom setup
        await OnPostSetupAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await CleanupDatabase();
        await Driver.DisposeAsync();
        Repo.Dispose();
        Host.Dispose();
    }

    /// <summary>
    /// Removes all nodes and relationships from the test database.
    /// </summary>
    protected async Task CleanupDatabase()
    {
        var session = Driver.AsyncSession(o => o.WithDatabase(TestDatabase));
        try
        {
            await session.ExecuteWriteAsync(async tx =>
            {
                var result = await tx.RunAsync("MATCH (n) DETACH DELETE n");
                await result.ConsumeAsync();
            });
        }
        finally
        {
            await session.CloseAsync();
        }
    }

    /// <summary>
    /// Ensures the test database exists (for Enterprise/AuraDB with multiple database support).
    /// </summary>
    private async Task EnsureDatabaseExists()
    {
        var session = Driver.AsyncSession(o => o.WithDatabase("system"));
        try
        {
            // Try to create database - IF NOT EXISTS means this is safe to run even if it already exists
            var result = await session.RunAsync($"CREATE DATABASE `{TestDatabase}` IF NOT EXISTS");
            await result.ConsumeAsync();
            Log.Information($"Ensured test database '{TestDatabase}' exists");

            // Wait a moment for database to be ready
            await Task.Delay(1000);
        }
        catch (ClientException ex) when (ex.Code.Contains("Unsupported"))
        {
            // Community Edition doesn't support multiple databases
            Log.Warning($"Multiple databases not supported (Community Edition). Please set TestDatabase = \"neo4j\" or upgrade to Enterprise/AuraDB.");
            Log.Warning($"Tests will attempt to use '{TestDatabase}' database anyway.");
        }
        catch (Exception ex)
        {
            Log.Error(ex, $"Failed to create database '{TestDatabase}'. Check your credentials match your Neo4j Desktop database.");
            Log.Information($"Current credentials: User='{Neo4jUser}', Uri='{Neo4jUri}'");
            throw;
        }
        finally
        {
            await session.CloseAsync();
        }
    }
}
