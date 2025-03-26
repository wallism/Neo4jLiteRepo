using Neo4jLiteRepo.Attributes;

namespace Neo4jLiteRepo.Sample
{
    /// <summary>
    /// Base class for all nodes in the Sample
    /// </summary>
    /// <remarks>Create similar for your project.
    /// Useful for properties you always want to include (e.g. SampleProperty here)</remarks>
    public abstract class SampleGraphNode : GraphNode
    {
        [NodeProperty(nameof(SampleProperty))]
        public virtual string? SampleProperty { get; set; }
    }

}
