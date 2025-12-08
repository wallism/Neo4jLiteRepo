using Neo4j.Driver;

namespace Neo4jLiteRepo.Interfaces;

/// <summary>
/// Embedding-specific operations for content chunks in Neo4j.
/// These operations are domain-specific for RAG/GraphRAG implementations.
/// </summary>
public interface IEmbeddingRepository
{
    /// <summary>
    /// Checks if the specified content chunk has an embedding vector.
    /// </summary>
    /// <param name="chunkId">The unique identifier of the content chunk.</param>
    /// <param name="tx">The active transaction.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>True if the chunk has an embedding; otherwise false.</returns>
    Task<bool> ContentChunkHasEmbeddingAsync(string chunkId, IAsyncTransaction tx, CancellationToken ct = default);

    /// <summary>
    /// Updates the embedding vector and hash for the specified content chunk.
    /// </summary>
    /// <param name="chunkId">The unique identifier of the content chunk.</param>
    /// <param name="vector">The embedding vector to store.</param>
    /// <param name="hash">Optional hash of the text used to generate the embedding (for change detection).</param>
    /// <param name="tx">The active transaction.</param>
    /// <param name="ct">Cancellation token.</param>
    Task UpdateChunkEmbeddingAsync(string chunkId, float[] vector, string? hash, IAsyncTransaction tx, CancellationToken ct = default);

    /// <summary>
    /// Retrieves the text and hash for the specified content chunk.
    /// </summary>
    /// <param name="chunkId">The unique identifier of the content chunk.</param>
    /// <param name="tx">The active transaction.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A tuple containing the chunk text and its hash (either may be null).</returns>
    Task<(string? Text, string? Hash)> GetChunkTextAndHashAsync(string chunkId, IAsyncTransaction tx, CancellationToken ct = default);
}
