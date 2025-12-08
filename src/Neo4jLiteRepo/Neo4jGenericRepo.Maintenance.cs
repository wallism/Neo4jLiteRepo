using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Exceptions;

namespace Neo4jLiteRepo;

/// <summary>
/// Maintenance operations for Neo4j - orphan removal, embedding stubs.
/// </summary>
public partial class Neo4jGenericRepo
{
    #region RemoveOrphansAsync

    /// <inheritdoc />
    public async Task<int> RemoveOrphansAsync<T>(CancellationToken ct = default) where T : GraphNode, new()
    {
        await using var session = _neo4jDriver.AsyncSession();
        // Delegate to the session overload to keep all write orchestration logic centralized.
        return await RemoveOrphansAsync<T>(session, ct);
    }

    /// <inheritdoc />
    public async Task<int> RemoveOrphansAsync<T>(IAsyncSession session, CancellationToken ct = default) where T : GraphNode, new()
    {
        if (session == null) throw new ArgumentNullException(nameof(session));
        await using var tx = await session.BeginTransactionAsync();
        try
        {
            var result = await RemoveOrphansAsync<T>(tx, ct);
            await tx.CommitAsync();
            return result;
        }
        catch
        {
            try
            {
                await tx.RollbackAsync();
            }
            catch
            {
                /* ignore */
            }

            throw;
        }
    }

    /// <inheritdoc />
    public async Task<int> RemoveOrphansAsync<T>(IAsyncTransaction tx, CancellationToken ct = default) where T : GraphNode, new()
    {
        if (tx == null) throw new ArgumentNullException(nameof(tx));
        var temp = new T();
        var label = temp.LabelName;
        ValidateLabel(label, nameof(label));

        // Batch delete orphans to avoid memory spikes
        const int batchSize = 400; // configurable if needed
        var totalDeleted = 0;
        var cypher = $$"""
                       MATCH (n:{{label}}) WHERE NOT (n)--() WITH n LIMIT $batchSize DETACH DELETE n RETURN count(n) AS deleted
                       """;

        try
        {
            while (true)
            {
                var cursor = await tx.RunAsync(cypher, new { batchSize });
                if (await cursor.FetchAsync())
                {
                    var deleted = cursor.Current["deleted"].As<int>();
                    totalDeleted += deleted;
                    if (deleted < batchSize)
                    {
                        // Last batch, done
                        break;
                    }
                }
                else
                {
                    break;
                }
            }

            return totalDeleted;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "RemoveOrphansAsync failure. QueryLength={QueryLength}", cypher.Length);
            throw new RepositoryException("Failed removing orphans.", cypher, ["label"], ex);
        }
    }

    #endregion

    #region Embedding Stubs

    // Domain-specific cascade deletes (like Section subtree) intentionally moved out to service layer.

    /// <inheritdoc/>
    public Task<bool> ContentChunkHasEmbeddingAsync(string chunkId, IAsyncTransaction tx, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task UpdateChunkEmbeddingAsync(string chunkId, float[] vector, string? hash, IAsyncTransaction tx, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task<(string? Text, string? Hash)> GetChunkTextAndHashAsync(string chunkId, IAsyncTransaction tx, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }

    #endregion
}
