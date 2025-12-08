using Microsoft.Extensions.Logging;
using Neo4j.Driver;

namespace Neo4jLiteRepo
{
    [Obsolete]
    internal interface INeo4jDataAccess : IAsyncDisposable
    {
        Task<List<string>> ExecuteReadListAsync(string query, string returnObjectKey, IDictionary<string, object>? parameters = null);

        Task<List<Dictionary<string, object>>> ExecuteReadDictionaryAsync(string query, string returnObjectKey, IDictionary<string, object>? parameters = null);

        Task<T> ExecuteReadScalarAsync<T>(string query, IDictionary<string, object>? parameters = null);

        Task<T> ExecuteWriteTransactionAsync<T>(string query, IDictionary<string, object>? parameters = null);
    }

    /// <summary>
    /// Data access class for Neo4j database operations.
    /// Initially taken from: https://neo4j.com/blog/developer/neo4j-data-access-for-your-dot-net-core-c-microservice/
    /// </summary>
    /// <remarks>included for quick reference only. Functionality to be moved into the generic repo as needed</remarks>
    internal class Neo4jDataAccess(ILogger<Neo4jDataAccess> logger,
        IDriver neo4jDriver) : INeo4jDataAccess
    {

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or
        /// resetting unmanaged resources asynchronously.
        /// </summary>
        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            neo4jDriver.Dispose();
        }
        /// <summary>
        /// Execute read list as an asynchronous operation.
        /// </summary>
        public async Task<List<string>> ExecuteReadListAsync(string query, string returnObjectKey, IDictionary<string, object>? parameters = null)
        {
            return await ExecuteReadTransactionAsync<string>(query, returnObjectKey, parameters);
        }

        /// <summary>
        /// Execute read dictionary as an asynchronous operation.
        /// </summary>
        public async Task<List<Dictionary<string, object>>> ExecuteReadDictionaryAsync(string query, string returnObjectKey, IDictionary<string, object>? parameters = null)
        {
            return await ExecuteReadTransactionAsync<Dictionary<string, object>>(query, returnObjectKey, parameters);
        }

        /// <summary>
        /// Execute read scalar as an asynchronous operation.
        /// </summary>
        public async Task<T> ExecuteReadScalarAsync<T>(string query, IDictionary<string, object>? parameters = null)
        {
            await using var session = neo4jDriver.AsyncSession();
            try
            {
                parameters ??= new Dictionary<string, object>();

                var result = await session.ExecuteReadAsync(async tx =>
                {
                    var res = await tx.RunAsync(query, parameters);

                    var scalar = (await res.SingleAsync())[0].As<T>();
                    return scalar;
                });

                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "There was a problem while executing database query");
                throw;
            }
        }

        /// <summary>
        /// Execute write transaction
        /// </summary>
        public async Task<T> ExecuteWriteTransactionAsync<T>(string query, IDictionary<string, object>? parameters = null)
        {
            await using var session = neo4jDriver.AsyncSession();
            try
            {
                parameters ??= new Dictionary<string, object>();

                var result = await session.ExecuteWriteAsync(async tx =>
                {
                    var res = await tx.RunAsync(query, parameters);

                    var scalar = (await res.SingleAsync())[0].As<T>();
                    return scalar;
                });

                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "There was a problem while executing database query");
                throw;
            }
        }

        /// <summary>
        /// Execute read transaction as an asynchronous operation.
        /// </summary>
        private async Task<List<T>> ExecuteReadTransactionAsync<T>(string query, string returnObjectKey, IDictionary<string, object>? parameters)
        {
            await using var session = neo4jDriver.AsyncSession();
            try
            {
                parameters ??= new Dictionary<string, object>();

                var result = await session.ExecuteReadAsync(async tx =>
                {

                    var res = await tx.RunAsync(query, parameters);

                    var records = await res.ToListAsync();

                    var data = records.Select(x => (T)x.Values[returnObjectKey]).ToList();
                    return data;
                });

                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "There was a problem while executing database query");
                throw;
            }
        }

    }
}
