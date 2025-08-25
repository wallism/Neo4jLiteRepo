using System.Diagnostics;

namespace Neo4jLiteRepo.Helpers
{
    public static class GraphNodeHelpers
    {
        /// <summary>
        /// De-duplicates a set of <see cref="GraphNode"/> instances by their primary key value while preserving insertion order.
        /// If a duplicate primary key is encountered with identical content, the later duplicate is ignored.
        /// If a duplicate primary key has different content, a deterministic variant key is created by appending a
        /// -DUP# suffix (incrementing until unique) and the mutated node is included.
        /// </summary>
        /// <param name="nodes">The full (possibly duplicated) set of nodes produced during structural traversal.</param>
        /// <returns>A list of nodes with adjusted unique primary key values where necessary.</returns>
        public static List<GraphNode> DeduplicateNodeIds(IEnumerable<GraphNode> nodes)
        {
            var uniqueNodes = new Dictionary<string, GraphNode>(StringComparer.Ordinal);
            var dedupeCounter = 1;
            foreach (var node in nodes)
            {
                var pkValue = node.GetPrimaryKeyValue();
                if (pkValue == null)
                    continue;
                if (!uniqueNodes.ContainsKey(pkValue))
                {
                    uniqueNodes[pkValue] = node;
                    continue;
                }

                var existing = uniqueNodes[pkValue];
                if (string.Equals(existing.GetMainContent(), node.GetMainContent(), StringComparison.Ordinal))
                {
                    // Content is the same, skip duplicate
                    Debug.Write($"Duplicate node PK detected (identical content - ignoring): {pkValue}");
                    continue;
                }

                // Content differs, generate a new unique PK
                string newPkValue;
                do
                {
                    newPkValue = $"{pkValue}-DUP{dedupeCounter}";
                    dedupeCounter++;
                } while (uniqueNodes.ContainsKey(newPkValue));
                // Optionally, set the new PK value on the node if the property is writable
                var pkProp = node.GetType().GetProperties().FirstOrDefault(p => p.GetCustomAttributes(typeof(Attributes.NodePrimaryKeyAttribute), false).Any());
                if (pkProp != null && pkProp.CanWrite)
                    pkProp.SetValue(node, newPkValue);
                uniqueNodes[newPkValue] = node;
                dedupeCounter = 1; // reset for next potential duplicate group
                Debug.Write($"Duplicate node PK detected (different content): {pkValue}, assigned new PK: {newPkValue}");
            }
            return uniqueNodes.Values.ToList();
        }
    }
}
