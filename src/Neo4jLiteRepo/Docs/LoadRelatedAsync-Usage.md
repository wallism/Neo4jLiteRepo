# LoadRelatedAsync - Loading Related Nodes WITH Edges

## Problem Statement

Previously, when you needed to load nodes based on a relationship traversal AND have their edges populated, you had two unsatisfying options:

1. **Use `ExecuteReadListAsync`** with custom Cypher - but this only returns the nodes without edges
2. **Use `LoadNodesViaPathNoEdgesAsync`** - same issue, no edge loading (formerly `LoadRelatedNodesAsync`)
3. **Load nodes first, then loop and call `LoadAsync` for each** - multiple round trips, inefficient

## Solution: `LoadRelatedAsync<TSource, TRelated>`

The new method combines relationship traversal with automatic edge loading, just like `LoadAllAsync` does.

### Example Use Case

**Before:**
```csharp
// Custom query returns Role nodes but WITHOUT their edges loaded
var roles = await _repo.ExecuteReadListAsync<Role>(
    "MATCH (role:Role)-[:FOR_TEAM]->(team:Team {id: $teamId}) RETURN role ORDER BY role.createdAt DESC",
    "role",
    new Dictionary<string, object> { { "teamId", teamId } }
);

// Edges are null/empty - would need additional queries per role
```

**After:**
```csharp
// Load related Role nodes WITH all their edges populated automatically
var roles = await _repo.LoadRelatedAsync<Team, Role>(
    sourceId: teamId,
    relationshipTypes: "FOR_TEAM",
    minHops: 1,
    maxHops: 1,
    direction: EdgeDirection.Incoming  // Role->Team, so from Team's perspective it's incoming
);

// All Role.Users, Role.Permissions etc. are now populated!
```

## Method Signature

```csharp
Task<IReadOnlyList<TRelated>> LoadRelatedAsync<TSource, TRelated>(
    string sourceId,
    string relationshipTypes,
    int minHops = 1,
    int maxHops = 1,
    bool includeEdgeObjects = false,
    IEnumerable<string>? includeEdges = null,
    EdgeDirection direction = EdgeDirection.Outgoing,
    IAsyncTransaction? tx = null,
    CancellationToken ct = default)
    where TSource : GraphNode, new()
    where TRelated : GraphNode, new();
```

## Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `sourceId` | Primary key value of the source node | Required |
| `relationshipTypes` | Pipe-delimited list (e.g. "FOR_TEAM\|ASSIGNED_TO") | Required |
| `minHops` | Minimum traversal depth (inclusive) | 1 |
| `maxHops` | Maximum traversal depth (inclusive, keep ?5) | 1 |
| `includeEdgeObjects` | Load CustomEdge objects with properties | false |
| `includeEdges` | Filter: only load specific edges | null (all) |
| `direction` | Outgoing, Incoming, or Both | Outgoing |
| `tx` | Optional transaction to participate in | null |
| `ct` | Cancellation token | default |

## Edge Direction Guide

Understanding direction is crucial:

```csharp
// Outgoing: (source)-[:REL]->(related)
// Example: Load all Teams that a User belongs to
var teams = await _repo.LoadRelatedAsync<User, Team>(
    userId, "MEMBER_OF", direction: EdgeDirection.Outgoing
);

// Incoming: (source)<-[:REL]-(related)
// Example: Load all Users that belong to a Team
var users = await _repo.LoadRelatedAsync<Team, User>(
    teamId, "MEMBER_OF", direction: EdgeDirection.Incoming
);

// Both: (source)-[:REL]-(related)  [undirected]
// Example: Load all connected nodes regardless of direction
var connected = await _repo.LoadRelatedAsync<Node, Node>(
    nodeId, "CONNECTED_TO", direction: EdgeDirection.Both
);
```

## Features

1. **Automatic edge loading** - All outgoing `List<string>` properties with `[NodeRelationship<T>]` attributes are populated
2. **Edge object support** - Can load `CustomEdge` collections with properties (set `includeEdgeObjects: true`)
3. **Selective edge loading** - Use `includeEdges` to filter which edge types to load
4. **Multi-hop traversal** - Support for variable-length paths (careful with performance)
5. **Transaction support** - Can participate in existing transactions
6. **Direction control** - Traverse outgoing, incoming, or both directions

## Comparison with Other Methods

| Method | Use When | Edges Loaded? |
|--------|----------|---------------|
| `ExecuteReadListAsync` | Custom Cypher, exact control | ? No |
| `LoadAsync` | Load single node by ID | ? Yes |
| `LoadAllAsync` | Load all nodes of a type | ? Yes |
| `LoadNodesViaPathNoEdgesAsync` | Multi-hop path traversal only | ? No |
| `LoadNodeIdsViaPathNoEdgesAsync` | Lightweight ID-only traversal | ? No |
| **`LoadRelatedAsync`** | **Load related nodes WITH edges** | **? Yes** |

## Performance Considerations

1. **Keep maxHops small** (?5) to avoid expensive traversals
2. **Use minHops=1, maxHops=1** for direct relationships (most common case)
3. **Filter edges** with `includeEdges` when you don't need all of them
4. **Transaction reuse** - Pass existing transaction when doing multiple operations

## Implementation Notes

- Uses the same `BuildLoadQuery` helper that powers `LoadAsync`/`LoadAllAsync`
- Ensures consistent behavior across all Load methods
- Validates relationship type names (alphanumeric + underscore only)
- Supports Neo4j's variable-length path syntax for multi-hop queries
- Applies DISTINCT to avoid duplicate results from multiple paths

## When NOT to Use

- **Complex graph queries** - Use `ExecuteReadListAsync` with custom Cypher
- **Aggregations** - Use custom queries with COUNT, SUM, etc.
- **Performance-critical paths** - Profile first; custom queries may be faster
- **Very deep traversals** (>5 hops) - Consider redesigning your graph structure
- **Path-only needs without edges** - Use `LoadNodesViaPathNoEdgesAsync` instead
