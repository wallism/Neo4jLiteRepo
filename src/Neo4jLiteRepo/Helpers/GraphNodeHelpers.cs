using System.Diagnostics;

namespace Neo4jLiteRepo.Helpers
{
    public static class GraphNodeHelpers
    {
        /// <summary>
        /// De-duplicates a set of <see cref="GraphNode"/> instances by their ID while preserving insertion order.
        /// If a duplicate ID is encountered with identical content, the later duplicate is ignored.
        /// If a duplicate ID has different content, a deterministic variant ID is created by appending a
        /// -DUP# suffix (incrementing until unique) and the mutated node is included.
        /// </summary>
        /// <param name="nodes">The full (possibly duplicated) set of nodes produced during structural traversal.</param>
        /// <returns>A list of nodes with adjusted unique IDs where necessary.</returns>
        public static List<GraphNode> DeduplicateNodeIds(IEnumerable<GraphNode> nodes)
        {
            var uniqueNodes = new Dictionary<string, GraphNode>(StringComparer.Ordinal);
            var dedupeCounter = 1;
            foreach (var node in nodes)
            {
                if (!uniqueNodes.ContainsKey(node.Id))
                {
                    uniqueNodes[node.Id] = node;
                    continue;
                }

                var existing = uniqueNodes[node.Id];
                if (string.Equals(existing.GetMainContent(), node.GetMainContent(), StringComparison.Ordinal))
                {
                    // Content is the same, skip duplicate
                    Debug.Write($"Duplicate node ID detected (identical content - ignoring): {node.Id}");
                    continue;
                }

                // Content differs, generate a new unique ID
                string newId;
                do
                {
                    newId = $"{node.Id}-DUP{dedupeCounter}";
                    dedupeCounter++;
                } while (uniqueNodes.ContainsKey(newId));
                node.Id = newId;
                uniqueNodes[newId] = node;
                dedupeCounter = 1; // reset for next potential duplicate group
                Debug.Write($"Duplicate node ID detected (different content): {node.Id}, assigned new ID: {newId}");
            }
            return uniqueNodes.Values.ToList();
        }
    }
}
