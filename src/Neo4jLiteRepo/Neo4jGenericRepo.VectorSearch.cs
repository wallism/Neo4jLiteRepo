using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using Neo4jLiteRepo.Exceptions;
using Neo4jLiteRepo.Models;

namespace Neo4jLiteRepo;

/// <summary>
/// Vector search operations for Neo4j.
/// </summary>
public partial class Neo4jGenericRepo
{
    #region ExecuteVectorSimilaritySearchAsync

    /// <summary>
    /// Executes a vector similarity search query to find relevant content chunks
    /// </summary>
    /// <param name="questionEmbedding">The embedding vector of the question</param>
    /// <param name="topK">Number of most relevant chunks to return</param>
    /// <param name="includeContext">Whether to include related chunks and parent context</param>
    /// <param name="similarityThreshold">Minimum cosine similarity threshold (0-1) for matching content. 
    /// Higher values (e.g. 0.8) will return only very close matches and may result in fewer results.
    /// Lower values (e.g. 0.5) will return more results but may include less relevant content.
    /// Values between 0.6-0.75 are typically a good starting point.</param>
    /// <returns>A list of strings containing the content and article information</returns>
    public async Task<List<string>> ExecuteVectorSimilaritySearchAsync(
        float[] questionEmbedding,
        int topK = 20,
        bool includeContext = true,
        double similarityThreshold = 0.65)
    {
        // Validate input
        if (questionEmbedding == null || questionEmbedding.Length == 0)
        {
            _logger.LogWarning("ExecuteVectorSimilaritySearchAsync called with null or empty embedding array");
            return [];
        }

        _logger.LogDebug("ExecuteVectorSimilaritySearchAsync called with embedding size={EmbeddingSize}, topK={TopK}, threshold={Threshold}", 
            questionEmbedding.Length, topK, similarityThreshold);

        var query = await GetCypherFromFile("ExecuteVectorSimilaritySearch.cypher", _logger);
        
        await using var session = StartSession();
        List<string> result;
        try
        {
            result = await session.ExecuteReadAsync(async tx =>
            {
                // Run the query
                // Note: Neo4j driver requires embedding as List, not array
                IResultCursor cursor;
                try
                {
                    cursor = await tx.RunAsync(query, new
                    {
                        questionEmbedding = questionEmbedding.ToList(),
                        topK,
                        similarityThreshold
                    });
                }
                catch (Exception exQuery)
                {
                    _logger.LogError(exQuery, "Failed to execute Neo4j query");
                    throw;
                }

                // Process results
                var resultsDict = new Dictionary<string, Dictionary<string, object>>();
                var recordCount = 0;

                await foreach (var record in cursor)
                {
                    recordCount++;
                    try
                    {
                        var id = record["id"].As<string>();

                    // If we've already seen this chunk, skip it (avoid duplicates)
                    if (resultsDict.ContainsKey(id))
                        continue;

                    resultsDict[id] = new Dictionary<string, object>
                    {
                        ["id"] = id,
                        ["content"] = record["content"].As<string>(),
                        ["articleTitle"] = record["articleTitle"].As<string>(),
                        ["articleUrl"] = record["articleUrl"].As<string>(),
                        ["score"] = record["mainScore"].As<double>(),
                        ["entities"] = record["mainEntities"].As<List<string>>(),
                        ["sequence"] = record["sequence"].As<int>(),
                        ["resultType"] = record["resultType"].As<string>()
                    };
                    }
                    catch (Exception exRecord)
                    {
                        _logger.LogWarning(exRecord, "Failed to process vector search record at position {RecordIndex}", recordCount);
                    }
                }

                _logger.LogInformation("Vector search returned {RecordCount} records, {UniqueChunks} unique chunks", 
                    recordCount, resultsDict.Count);

                // Sort by article title and sequence order for better readability
                var sortedResults = resultsDict.Values
                    .OrderBy(r => r["articleTitle"].ToString())
                    .ThenBy(r => (int)r["sequence"])
                    .ToList();

                // Format results to return
                List<string> formattedResults = [];

                var currentArticle = "";
                foreach (var r in sortedResults)
                {
                    var articleTitle = r["articleTitle"]?.ToString() ?? "Unknown Article";
                    var articleUrl = r["articleUrl"]?.ToString() ?? "no-link";

                    // If we're starting a new article, add a header
                    if (articleTitle != currentArticle)
                    {
                        if (formattedResults.Count > 0)
                            formattedResults.Add($"-- end article: {currentArticle} --"); // Add a clear delimiter between articles for LLM context

                        formattedResults.Add($"-- start article: {articleTitle} --");
                        formattedResults.Add($"article url: {articleUrl}");
                        currentArticle = articleTitle;
                    }

                    // Add content with prefix based on result type
                    var prefix = "";
                    var resultType = r["resultType"]?.ToString() ?? "unknown";

                    if (resultType == "main")
                        prefix = "▶️ "; // Highlight the main matches
                    else if (resultType == "next")
                        prefix = "⏩ "; // Context that follows
                    else if (resultType == "previous")
                        prefix = "⏪ "; // Context that precedes
                    else if (resultType == "sibling")
                        prefix = "🔄 "; // Related content
                    else if (resultType == "section_related")
                        prefix = "📑 "; // Section-related content
                    else if (resultType == "subsection_related")
                        prefix = "📋 "; // SubSection-related content

                    var entities = r["entities"] as List<string> ?? [];
                    var entityInfo = entities.Any() ? $" [Entities: {string.Join(", ", entities)}]" : "";

                    var content = $"{prefix}{r["content"]}{entityInfo}";
                    if (!formattedResults.Contains(content) && !string.IsNullOrWhiteSpace(content))
                        formattedResults.Add(content);
                }

                return formattedResults;
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Vector similarity search failed. QueryLength={QueryLength} ParamKeys=questionEmbedding,topK", query.Length);
            throw new RepositoryException("Vector similarity search failed.", query, ["questionEmbedding", "topK"], ex);
        }

        return result;
    }

    #endregion

    #region ExecuteVectorSimilaritySearchStructuredAsync

    /// <summary>
    /// Structured variant of vector similarity search. Returns a flat list of result rows with consistent fields.
    /// </summary>
    public async Task<IReadOnlyList<StructuredVectorSearchRow>> ExecuteVectorSimilaritySearchStructuredAsync(
        float[] questionEmbedding,
        int topK = 20,
        double similarityThreshold = 0.65)
    {
        var query = await GetCypherFromFile("ExecuteVectorSimilaritySearchStructured.cypher", _logger);
        await using var session = StartSession();
        try
        {
            var rows = await session.ExecuteReadAsync(async tx =>
            {
                var cursor = await tx.RunAsync(query, new
                {
                    questionEmbedding = questionEmbedding.ToList(),
                    topK,
                    similarityThreshold
                });

                List<StructuredVectorSearchRow> list = [];
                await foreach (var record in cursor)
                {
                    try
                    {
                        list.Add(new StructuredVectorSearchRow
                        {
                            ChunkId = record["chunkId"].As<string>(),
                            ArticleTitle = record["articleTitle"].As<string>(),
                            ArticleUrl = record["articleUrl"].As<string>(),
                            SnippetType = record["snippetType"].As<string>(),
                            Content = record["content"].As<string>(),
                            BaseScore = record["baseScore"].As<double>(),
                            Sequence = record["sequence"].As<int>(),
                            Entities = record["entities"].As<List<string>>()
                        });
                    }
                    catch (Exception exInner)
                    {
                        _logger.LogWarning(exInner, "Failed to materialize structured vector search row");
                    }
                }
                return (IReadOnlyList<StructuredVectorSearchRow>)list;
            });
            return rows;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Structured vector similarity search failed. QueryLength={QueryLength} ParamKeys=questionEmbedding,topK", query.Length);
            throw new RepositoryException("Structured vector similarity search failed.", query, ["questionEmbedding", "topK"], ex);
        }
    }

    #endregion
}
