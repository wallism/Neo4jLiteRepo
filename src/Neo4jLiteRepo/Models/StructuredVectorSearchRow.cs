namespace Neo4jLiteRepo.Models;

/// <summary>
/// Row returned from structured vector similarity search.
/// </summary>
public sealed class StructuredVectorSearchRow
{
    public string ChunkId { get; set; } = string.Empty;
    public string ArticleTitle { get; set; } = string.Empty;
    public string ArticleUrl { get; set; } = string.Empty;
    public string SnippetType { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;
    public double BaseScore { get; set; }
    public int Sequence { get; set; }
    public List<string> Entities { get; set; } = new();
}
