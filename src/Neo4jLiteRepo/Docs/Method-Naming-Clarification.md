# Method Naming Clarification - LoadRelated* Methods

## Summary

Renamed existing path-traversal methods to clearly indicate they do NOT load edges, distinguishing them from the new full-featured `LoadRelatedAsync` method.

## Renamed Methods

| Old Name | New Name | What It Does |
|----------|----------|--------------|
| `LoadRelatedNodesAsync` | **`LoadNodesViaPathNoEdgesAsync`** | Multi-hop path traversal, returns nodes WITHOUT edges |
| `LoadRelatedNodeIdsAsync` | **`LoadNodeIdsViaPathNoEdgesAsync`** | Multi-hop path traversal, returns IDs only (no nodes, no edges) |

## Why the Rename?

The original names were too similar to the new `LoadRelatedAsync` method, creating confusion:

- **Old**: `LoadRelatedNodesAsync` vs `LoadRelatedAsync` - what's the difference?
- **New**: `LoadNodesViaPathNoEdgesAsync` vs `LoadRelatedAsync` - crystal clear!

The new names emphasize:
1. **ViaPath** - Multi-hop path traversal is the focus
2. **NoEdges** - Explicitly states edges are NOT loaded
3. **Nodes/NodeIds** - Clarifies what is returned

## Method Comparison Matrix

| Method | Returns | Edges Loaded? | Direction Support? | Primary Use Case |
|--------|---------|---------------|-------------------|------------------|
| `LoadAsync<T>` | Single node | ? Yes | N/A | Load one specific node with all edges |
| `LoadAllAsync<T>` | All nodes of type | ? Yes | N/A | Load all nodes with edges (paginated) |
| **`LoadRelatedAsync<TSource, TRelated>`** | **Related nodes** | **? Yes** | **? Yes** | **Load related nodes WITH edges** |
| `LoadNodesViaPathNoEdgesAsync<TSource, TRelated>` | Related nodes | ? No | ? Outgoing only | Path traversal without edge overhead |
| `LoadNodeIdsViaPathNoEdgesAsync<TRelated>` | IDs only | ? No | ? Yes | Lightweight traversal for cascade ops |
| `ExecuteReadListAsync<T>` | Custom query results | ? No | N/A | Full Cypher control |

## Examples

### LoadRelatedAsync (NEW - With Edges)
```csharp
// Load all Roles for a Team, WITH their edges populated
var roles = await repo.LoadRelatedAsync<Team, Role>(
    teamId, 
    "FOR_TEAM", 
    direction: EdgeDirection.Incoming
);

// roles[0].UserIds is populated ?
// roles[0].PermissionIds is populated ?
```

### LoadNodesViaPathNoEdgesAsync (Renamed - No Edges)
```csharp
// Load ContentChunks 0-4 hops from Article via content relationships
// Fast path traversal but edges NOT loaded
var chunks = await repo.LoadNodesViaPathNoEdgesAsync<Article, ContentChunk>(
    articleId,
    "HAS_SECTION|HAS_SUB_SECTION|CONTAINS",
    minHops: 0,
    maxHops: 4
);

// chunks[0].ReferencesEntity is empty ? (edges not loaded)
```

### LoadNodeIdsViaPathNoEdgesAsync (Renamed - IDs Only)
```csharp
// Get entity IDs derived from an article (incoming direction)
// Ultra-lightweight - no node hydration, no edges
var entityIds = await repo.LoadNodeIdsViaPathNoEdgesAsync<Entity>(
    articleNode,
    "DERIVED_FROM",
    direction: EdgeDirection.Incoming
);

// Returns: ["entity-1", "entity-2", "entity-3"]
// No Entity objects created, no edges loaded
```

## Migration Guide

### Update Your Code

**Before:**
```csharp
var nodes = await repo.LoadRelatedNodesAsync<Source, Target>(...);
var ids = await repo.LoadRelatedNodeIdsAsync<Target>(...);
```

**After:**
```csharp
var nodes = await repo.LoadNodesViaPathNoEdgesAsync<Source, Target>(...);
var ids = await repo.LoadNodeIdsViaPathNoEdgesAsync<Target>(...);
```

### When to Use Which?

**Use `LoadRelatedAsync`** when:
- ? You need related nodes AND their edges
- ? You want the same behavior as `LoadAsync`/`LoadAllAsync`
- ? You're replacing custom `ExecuteReadListAsync` queries
- ? Example: Loading Roles for a Team with all Role relationships

**Use `LoadNodesViaPathNoEdgesAsync`** when:
- ? You only need the target nodes, no edges
- ? You're doing multi-hop traversal (2+ hops)
- ? Performance is critical and you don't need edge data
- ? Example: Finding all chunks in an article hierarchy

**Use `LoadNodeIdsViaPathNoEdgesAsync`** when:
- ? You only need IDs for further processing
- ? You're doing cascade operations (delete related, check orphans)
- ? Maximum performance (no node hydration)
- ? Example: Finding entity IDs to remove edges

**Use `ExecuteReadListAsync`** when:
- ? You need custom Cypher (aggregations, complex logic)
- ? None of the Load methods fit your use case

## Design Principles

1. **Explicit is better than implicit** - "NoEdges" leaves no room for confusion
2. **Consistency** - `Load*` methods that populate edges should have similar behavior
3. **Verbosity acceptable for rarely-used APIs** - These path methods are less common than Load/LoadAll
4. **Performance clarity** - Name indicates what work is/isn't being done

## Updated in This Change

? **Interface**: `INeo4jGenericRepo`  
? **Implementation**: `Neo4jGenericRepo.Relationships.cs`  
? **Interface**: `INeo4jRelationshipRepository`  
? **Usage**: `ArticleMapSyncService.cs` (2 call sites)  
? **Documentation**: `LoadRelatedAsync-Usage.md`  
? **Build**: Verified successful compilation  

## No Breaking Changes For

- `LoadAsync` - unchanged
- `LoadAllAsync` - unchanged
- `LoadRelatedAsync` - NEW, no breaking change
- `ExecuteRead*` methods - unchanged

All existing Load methods continue to work as expected with consistent edge-loading behavior.
