using Neo4j.Driver;

namespace Neo4jLiteRepo.Helpers
{
    public static class Neo4jWriteExtensions
    {
        /// <summary>
        /// Runs a write-only query and ensures it is fully consumed.
        /// Returns the ResultSummary so you can inspect counters, stats, etc.
        /// </summary>
        public static async Task<IResultSummary> RunWriteAsync(
            this IAsyncQueryRunner runner,
            string query,
            object? parameters = null)
        {
            var cursor = await runner.RunAsync(query, parameters);
            return await cursor.ConsumeAsync(); // ensures query is fully executed
        }
    }

}
