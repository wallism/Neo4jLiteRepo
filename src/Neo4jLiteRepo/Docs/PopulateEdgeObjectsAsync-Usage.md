# PopulateEdgeObjectsAsync - Hydrating Object Collections

## Problem Statement

When you load a node using `LoadAsync` or `LoadAllAsync`, you get:
- ? The node's properties
- ? Edge ID lists (e.g., `Topics` as `List<string>`)
- ? Edge object collections (e.g., `CoveredTopics` as `List<Topic>`)

This leaves you with a partially hydrated object where you have the IDs but not the actual related objects.

## Solution: `PopulateEdgeObjectsAsync<T>`

This method takes a node that already has edge ID lists populated and hydrates the corresponding object collections.

### Before & After Example

**Article Model:**
```csharp
public class Article : GraphNode
{
    // Edge ID list - populated by LoadAsync
    [NodeRelationship<Topic>("COVERS")]
    public List<string> Topics { get; set; } = [];

    // Edge object collection - NOT populated by LoadAsync
    public List<Topic> CoveredTopics { get; set; } = [];

    // Another edge relationship
    [NodeRelationship<Section>("HAS_SECTION")]
    public List<string> RelatedSections { get; set; } = [];

    // Section object collection
    public List<Section> Sections { get; set; } = [];
}
```

**Before (LoadAsync only):**
```csharp
var article = await repo.LoadAsync<Article>(articleId);

// ? article.Topics has IDs: ["topic-1", "topic-2", "topic-3"]
// ? article.CoveredTopics is empty: []
// ? article.RelatedSections has IDs: ["section-1", "section-2"]
// ? article.Sections is empty: []
```

**After (with PopulateEdgeObjectsAsync):**
```csharp
var article = await repo.LoadAsync<Article>(articleId);
await repo.PopulateEdgeObjectsAsync(article);

// ? article.Topics has IDs: ["topic-1", "topic-2", "topic-3"]
// ? article.CoveredTopics has objects: [Topic{Id="topic-1"}, Topic{Id="topic-2"}, ...]
// ? article.RelatedSections has IDs: ["section-1", "section-2"]
// ? article.Sections has objects: [Section{Id="section-1"}, Section{Id="section-2"}]
```

## Method Signature

```csharp
Task PopulateEdgeObjectsAsync<T>(
    T node, 
    IEnumerable<string>? edgesToLoad = null, 
    IAsyncTransaction? tx = null, 
    CancellationToken ct = default)
    where T : GraphNode;
```

## Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `node` | Node instance with ID lists populated | Required |
| `edgesToLoad` | Specific edge properties to load (null = all) | null (all edges) |
| `tx` | Optional transaction | null |
| `ct` | Cancellation token | default |

## Usage Examples

### Load All Edges

```csharp
var article = await repo.LoadAsync<Article>(articleId);
await repo.PopulateEdgeObjectsAsync(article);

// All edge object collections are now populated
foreach (var topic in article.CoveredTopics)
{
    Console.WriteLine($"Topic: {topic.Name}");
}

foreach (var section in article.Sections)
{
    Console.WriteLine($"Section: {section.Title}");
}
```

### Load Specific Edges Only

```csharp
var article = await repo.LoadAsync<Article>(articleId);

// Only load CoveredTopics, skip Sections
await repo.PopulateEdgeObjectsAsync(article, ["CoveredTopics"]);

// ? article.CoveredTopics is populated
// ? article.Sections remains empty (not loaded)
```

### With Transaction

```csharp
await using var session = repo.StartSession();
await using var tx = await session.BeginTransactionAsync();

var article = await repo.LoadAsync<Article>(articleId);
await repo.PopulateEdgeObjectsAsync(article, tx: tx);

// All edge objects loaded within the same transaction
await tx.CommitAsync();
```

### Batch Processing

```csharp
var articles = await repo.LoadAllAsync<Article>();

foreach (var article in articles)
{
    await repo.PopulateEdgeObjectsAsync(article, ["CoveredTopics"]);
    // Process article with populated topics
}
```

## How It Works

### Discovery Process

1. **Find Relationship Properties**
   - Scans for properties with `[NodeRelationship<T>]` attribute
   - Identifies the target type (e.g., `Topic`, `Section`)

2. **Extract IDs**
   - Gets the ID list from the relationship property (e.g., `Topics`)
   - Filters out null/empty IDs

3. **Batch Load Objects**
   - Executes single query: `MATCH (n:Label) WHERE n.id IN $ids RETURN n`
   - Maps results to target type instances

4. **Find Target Collection**
   - Searches for object collection property using naming patterns:
     - "Covered" + TypeName (e.g., `CoveredTopics`)
     - Plural TypeName (e.g., `Sections`)
     - Any `List<TargetType>` property

5. **Populate Collection**
   - Sets the object collection property with loaded objects

### Naming Convention Support

The method supports multiple naming patterns to find object collection properties:

| ID Property | Target Type | Possible Object Properties |
|-------------|-------------|---------------------------|
| `Topics` | `Topic` | `CoveredTopics`, `Topics` (if List<Topic>) |
| `RelatedSections` | `Section` | `CoveredSections`, `Sections` |
| `UserIds` | `User` | `CoveredUsers`, `Users` |

## Performance Considerations

### Batch Loading

? **One query per edge type** - not per ID
```csharp
// Loads all topics in ONE query:
UNWIND ["topic-1", "topic-2", "topic-3"] AS id
MATCH (n:Topic { id: id })
RETURN n
```

? **NOT N+1 queries** - doesn't loop through IDs

### Memory Efficiency

- Loads only requested edge types
- Reuses existing ID lists (no duplicate storage)
- Works with transactions for batch processing

### When to Use

| Scenario | Recommended Approach |
|----------|---------------------|
| **Need objects immediately** | Use `LoadRelatedAsync` (one query, edges included) |
| **Conditional loading** | Load with `LoadAsync`, then `PopulateEdgeObjectsAsync` |
| **Selective edges** | `PopulateEdgeObjectsAsync(node, ["SpecificEdge"])` |
| **Many nodes, few need edges** | Load all with `LoadAllAsync`, populate selectively |

## Comparison with Other Methods

| Method | When IDs Populated | When Objects Populated | Queries |
|--------|-------------------|------------------------|---------|
| `LoadAsync` | ? Always | ? Never | 1 |
| `LoadAsync` + `PopulateEdgeObjectsAsync` | ? First | ? Second | 1 + N edge types |
| `LoadRelatedAsync` | ? Together | ? Together | 1 |

## Example: Article Processing Workflow

```csharp
public async Task ProcessArticlesAsync()
{
    // Step 1: Load all articles (fast, IDs only)
    var articles = await _repo.LoadAllAsync<Article>();
    
    // Step 2: Filter articles that need topic processing
    var articlesNeedingTopics = articles
        .Where(a => a.Topics.Any(t => t.StartsWith("important-")))
        .ToList();
    
    // Step 3: Populate only for filtered articles
    foreach (var article in articlesNeedingTopics)
    {
        await _repo.PopulateEdgeObjectsAsync(article, ["CoveredTopics"]);
        
        // Now process with full Topic objects
        foreach (var topic in article.CoveredTopics)
        {
            Console.WriteLine($"Processing {article.Title} - {topic.Name}");
        }
    }
}
```

## Error Handling

### Missing Object Collection Property

```csharp
// If Article doesn't have a CoveredTopics property:
await repo.PopulateEdgeObjectsAsync(article);

// Logs warning:
// "Could not find object collection property for Topics (target type: Topic) on Article"
// Continues processing other edges
```

### Invalid IDs

```csharp
article.Topics = ["valid-id", "nonexistent-id"];
await repo.PopulateEdgeObjectsAsync(article);

// Only loads objects that exist in database
// No error thrown for missing IDs
```

## Best Practices

### 1. Use for Conditional Loading

```csharp
var article = await repo.LoadAsync<Article>(articleId);

if (includeTopics)
{
    await repo.PopulateEdgeObjectsAsync(article, ["CoveredTopics"]);
}

if (includeSections)
{
    await repo.PopulateEdgeObjectsAsync(article, ["Sections"]);
}
```

### 2. Combine with Filtering

```csharp
var articles = await repo.LoadAllAsync<Article>();

// Only populate edges for articles that need it
var recentArticles = articles.Where(a => a.Upserted > cutoffDate);
foreach (var article in recentArticles)
{
    await repo.PopulateEdgeObjectsAsync(article);
}
```

### 3. Use Transactions for Consistency

```csharp
await using var session = repo.StartSession();
await using var tx = await session.BeginTransactionAsync();

var article = await repo.LoadAsync<Article>(articleId);
await repo.PopulateEdgeObjectsAsync(article, tx: tx);

// Ensure article and edges read from same snapshot
await tx.CommitAsync();
```

### 4. Be Selective with Large Collections

```csharp
// DON'T load all edges if you only need one
await repo.PopulateEdgeObjectsAsync(article); // ? Loads everything

// DO specify only what you need
await repo.PopulateEdgeObjectsAsync(article, ["CoveredTopics"]); // ? Only topics
```

## Integration with Existing Code

### Before (Manual Loading)

```csharp
var article = await repo.LoadAsync<Article>(articleId);

// Manually load topics
var topics = new List<Topic>();
foreach (var topicId in article.Topics)
{
    var topic = await repo.LoadAsync<Topic>(topicId);  // N+1 queries!
    if (topic != null) topics.Add(topic);
}
article.CoveredTopics = topics;

// Manually load sections
var sections = new List<Section>();
foreach (var sectionId in article.RelatedSections)
{
    var section = await repo.LoadAsync<Section>(sectionId);  // N+1 queries!
    if (section != null) sections.Add(section);
}
article.Sections = sections;
```

### After (Using PopulateEdgeObjectsAsync)

```csharp
var article = await repo.LoadAsync<Article>(articleId);
await repo.PopulateEdgeObjectsAsync(article);  // 2 batch queries (one per edge type)

// Done! article.CoveredTopics and article.Sections are populated
```

## When NOT to Use

? **Don't use when:**
1. You always need the objects (use `LoadRelatedAsync` instead)
2. The edge objects have their own edges you need (recursive loading not supported)
3. You're loading many nodes and all need edges (use `LoadRelatedAsync` with pagination)

? **DO use when:**
1. You conditionally need edge objects
2. You filter nodes before populating edges
3. You want fine-grained control over which edges to load
4. You're working with existing code that uses `LoadAsync`

## Future Enhancements

Possible future improvements (not currently implemented):
- Recursive edge loading (load edges of edges)
- Caching loaded objects across calls
- Parallel batch loading for multiple edge types
- Support for edge object properties (CustomEdge with metadata)
