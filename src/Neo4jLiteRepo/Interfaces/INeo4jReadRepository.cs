using Neo4j.Driver;

namespace Neo4jLiteRepo.Interfaces;

/// <summary>
/// Read-only query operations for Neo4j.
/// </summary>
public interface INeo4jReadRepository
{
    /// <summary>
    /// Executes a read query and returns a list of objects of type T.
    /// </summary>
    Task<IEnumerable<T>> ExecuteReadListAsync<T>(string query, string returnObjectKey,
        IDictionary<string, object>? parameters = null)
        where T : class, new();

    /// <summary>
    /// Executes a read query and returns a list of objects of type T using the provided session.
    /// </summary>
    Task<IEnumerable<T>> ExecuteReadListAsync<T>(string query, string returnObjectKey,
        IAsyncSession session, IDictionary<string, object>? parameters = null)
        where T : class, new();

    /// <summary>
    /// Streams results without materializing entire result set in memory. 
    /// Caller should enumerate promptly; the underlying session is disposed when enumeration completes.
    /// </summary>
    IAsyncEnumerable<T> ExecuteReadStreamAsync<T>(string query, string returnObjectKey, IDictionary<string, object>? parameters = null)
        where T : class, new();

    /// <summary>
    /// Executes a read query and returns a list of strings from the result.
    /// </summary>
    Task<IEnumerable<string>> ExecuteReadListStringsAsync(string query, string returnObjectKey, IDictionary<string, object>? parameters = null);

    /// <summary>
    /// Executes a read query and returns a scalar value of type T.
    /// </summary>
    Task<T> ExecuteReadScalarAsync<T>(string query, IDictionary<string, object>? parameters = null);
}
