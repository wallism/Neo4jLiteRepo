using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Exceptions;

namespace Neo4jLiteRepo;

/// <summary>
/// Read operations for Neo4j queries.
/// </summary>
public partial class Neo4jGenericRepo
{
    #region ExecuteReadListStringsAsync

    /// <inheritdoc/>
    public async Task<IEnumerable<string>> ExecuteReadListStringsAsync(string query, string returnObjectKey, IDictionary<string, object>? parameters = null)
    {
        await using var session = StartSession();
        try
        {
            parameters ??= new Dictionary<string, object>();

            var result = await session.ExecuteReadAsync(async tx =>
            {
                var records = await RunReadQueryValidateAlias(tx, query, parameters, returnObjectKey);
                if (records.Count == 0)
                    return [];

                // Assuming the returned value is a list of objects (like ["a", "b", "c"])
                var list = records
                    .SelectMany(x => x[returnObjectKey].As<List<string>>())
                    .Distinct()
                    .ToList();
                return list;
            });

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Problem executing read list of strings. QueryLength={QueryLength} ParamKeys={ParamKeys}", query.Length,
                string.Join(',', parameters?.Keys ?? []));
            throw new RepositoryException("Read list (string) query failed.", query, parameters?.Keys ?? [], ex);
        }
    }

    #endregion

    #region ExecuteReadListAsync

    /// <inheritdoc/>
    public async Task<IEnumerable<T>> ExecuteReadListAsync<T>(string query,
        string returnObjectKey, IDictionary<string, object>? parameters = null)
        where T : class, new()
    {
        // Maintains existing API while improving memory profile (no ToListAsync full materialization) and using compiled mapper
        await using var session = StartSession();
        return await ExecuteReadListAsync<T>(query, returnObjectKey, session, parameters);
    }

    /// <inheritdoc/>
    public async Task<IEnumerable<T>> ExecuteReadListAsync<T>(string query,
        string returnObjectKey, IAsyncSession session, IDictionary<string, object>? parameters = null)
        where T : class, new()
    {
        try
        {
            parameters ??= new Dictionary<string, object>();

            var result = await session.ExecuteReadAsync(async tx =>
            {
                var cursor = await tx.RunAsync(query, parameters);
                List<T> list = [];
                var aliasValidated = false;
                var idProp = typeof(T).GetProperties()
                    .FirstOrDefault(p => string.Equals(p.Name, "Id", StringComparison.OrdinalIgnoreCase) && p.GetMethod != null);
                var seen = idProp != null ? new HashSet<string>(StringComparer.OrdinalIgnoreCase) : null;

                while (await cursor.FetchAsync())
                {
                    var record = cursor.Current;
                    if (!aliasValidated)
                    {
                        if (!record.Keys.Contains(returnObjectKey))
                        {
                            var available = string.Join(", ", record.Keys);
                            throw new KeyNotFoundException(
                                $"Return alias '{returnObjectKey}' not found. Available aliases: {available}. Ensure your Cypher uses 'RETURN <expr> AS {returnObjectKey}'. Query={query}");
                        }

                        aliasValidated = true;
                    }

                    var node = record[returnObjectKey].As<INode>();
                    var obj = MapNodeToObject<T>(node); // uses compiled mapper internally
                    if (seen != null)
                    {
                        var valObj = idProp!.GetValue(obj);
                        var key = valObj?.ToString() ?? string.Empty;
                        if (key.Length > 0 && !seen.Add(key))
                            continue; // skip duplicate
                    }

                    list.Add(obj);
                }

                return list;
            });

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Problem executing read list. QueryLength={QueryLength} ParamKeys={ParamKeys}", query.Length, string.Join(',', parameters?.Keys ?? []));
            throw new RepositoryException("Read list query failed.", query, parameters?.Keys ?? [], ex);
        }
    }

    #endregion

    #region ExecuteReadStreamAsync

    /// <inheritdoc/>
    public async IAsyncEnumerable<T> ExecuteReadStreamAsync<T>(string query, string returnObjectKey, IDictionary<string, object>? parameters = null)
        where T : class, new()
    {
        // WARNING: If the consumer does not fully enumerate the stream, the session may not be disposed promptly.
        // Use 'await using' to ensure session disposal and prevent memory leaks.
        parameters ??= new Dictionary<string, object>();
        await using var session = StartSession();
        IResultCursor? cursor = null;
        try
        {
            cursor = await session.RunAsync(query, parameters);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Problem starting streamed read. QueryLength={QueryLength} ParamKeys={ParamKeys}", query.Length, string.Join(',', parameters.Keys));
            throw new RepositoryException("Read stream query failed (initialization).", query, parameters.Keys, ex);
        }

        var aliasValidated = false;
        while (await cursor.FetchAsync())
        {
            var record = cursor.Current;
            if (!aliasValidated)
            {
                if (!record.Keys.Contains(returnObjectKey))
                {
                    var available = string.Join(", ", record.Keys);
                    throw new KeyNotFoundException(
                        $"Return alias '{returnObjectKey}' not found. Available aliases: {available}. Ensure your Cypher uses 'RETURN <expr> AS {returnObjectKey}'. Query={query}");
                }

                aliasValidated = true;
            }

            var node = record[returnObjectKey].As<INode>();
            yield return MapNodeToObject<T>(node);
        }
    }

    #endregion

    #region ExecuteReadScalarAsync

    /// <summary>
    /// Execute read scalar as an asynchronous operation.
    /// </summary>
    /// <remarks>untested - 20250424</remarks>
    public async Task<T> ExecuteReadScalarAsync<T>(string query, IDictionary<string, object>? parameters = null)
    {
        await using var session = StartSession();
        try
        {
            parameters ??= new Dictionary<string, object>();

            var result = await session.ExecuteReadAsync(async tx =>
            {
                var cursor = await tx.RunAsync(query, parameters);
                var scalar = (await cursor.SingleAsync())[0].As<T>();
                return scalar;
            });

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Problem executing scalar read. QueryLength={QueryLength} ParamKeys={ParamKeys}", query.Length, string.Join(',', parameters?.Keys ?? []));
            throw new RepositoryException("Read scalar query failed.", query, parameters?.Keys ?? [], ex);
        }
    }

    #endregion

    #region ExecuteReadNodeQueryAsync

    /// <summary>
    /// Reusable internal helper to execute a read query expected to return a single node per record
    /// (under a specified alias) and map results to <typeparamref name="T"/> using the compiled mapper.
    /// Distinct filtering by Id (if present) is applied client-side. Intended for lightweight list lookups.
    /// </summary>
    /// <typeparam name="T">Graph node type to materialize.</typeparam>
    /// <param name="query">Cypher query text.</param>
    /// <param name="parameters">Query parameters (nullable -&gt; empty).</param>
    /// <param name="returnAlias">Alias of the node in the RETURN clause (default 'node').</param>
    /// <param name="runner">Optional existing transaction/session runner; if null a temp session is created.</param>
    /// <param name="ct">Cancellation token. Currently unused as Neo4j driver's RunAsync does not accept cancellation tokens directly.
    /// Retained for API consistency and future driver support.</param>
    /// <remarks>
    /// TODO: Monitor Neo4j .NET driver updates for native CancellationToken support in query execution.
    /// See: https://github.com/neo4j/neo4j-dotnet-driver/issues for tracking.
    /// </remarks>
    private async Task<IReadOnlyList<T>> ExecuteReadNodeQueryAsync<T>(string query, IDictionary<string, object>? parameters, string returnAlias, IAsyncQueryRunner? runner, CancellationToken ct)
        where T : GraphNode
    {
        parameters ??= new Dictionary<string, object>();

        async Task<IReadOnlyList<T>> InnerAsync(IAsyncQueryRunner r)
        {
            try
            {
                var cursor = await r.RunAsync(query, parameters);
                List<T> list = [];
                while (await cursor.FetchAsync())
                {
                    var record = cursor.Current;
                    if (!record.Keys.Contains(returnAlias)) continue;
                    var node = record[returnAlias].As<INode>();
                    var mapped = MapNodeToObject<T>(node);
                    list.Add(mapped);
                }

                var idProp = typeof(T).GetProperty("Id", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
                if (idProp != null)
                {
                    list = list
                        .GroupBy(o => idProp.GetValue(o)?.ToString(), StringComparer.OrdinalIgnoreCase)
                        .Select(g => g.First())
                        .ToList();
                }

                return list;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "ExecuteReadNodeQueryAsync failure. Alias={Alias} QueryLength={QueryLength}", returnAlias, query.Length);
                throw new RepositoryException("Failed executing node list read.", query, parameters.Keys, ex);
            }
        }

        if (runner != null)
        {
            return await InnerAsync(runner);
        }

        await using var session = StartSession();
        return await session.ExecuteReadAsync(async tx => await InnerAsync(tx));
    }

    #endregion
}
