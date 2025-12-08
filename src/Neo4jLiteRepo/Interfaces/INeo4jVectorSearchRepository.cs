using Neo4jLiteRepo.Models;

namespace Neo4jLiteRepo.Interfaces;

/// <summary>
/// Vector/semantic search operations for Neo4j.
/// </summary>
public interface INeo4jVectorSearchRepository
{
    /// <summary>
    /// Executes a vector similarity search query to find relevant content chunks.
    /// </summary>
    /// <param name="questionEmbedding">The embedding vector of the question</param>
    /// <param name="topK">Number of most relevant chunks to return</param>
    /// <param name="includeContext">Whether to include related chunks and parent context</param>
    /// <param name="similarityThreshold">Minimum cosine similarity threshold (0-1) for matching content</param>
    /// <returns>A list of strings containing the content and article information</returns>
    Task<List<string>> ExecuteVectorSimilaritySearchAsync(
        float[] questionEmbedding,
        int topK = 5,
        bool includeContext = true,
        double similarityThreshold = 0.6);

    /// <summary>
    /// Executes the structured vector similarity search returning strongly typed rows.
    /// </summary>
    Task<IReadOnlyList<StructuredVectorSearchRow>> ExecuteVectorSimilaritySearchStructuredAsync(
        float[] questionEmbedding,
        int topK = 20,
        double similarityThreshold = 0.65);
}
