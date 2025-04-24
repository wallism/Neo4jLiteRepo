using Neo4jLiteRepo.Helpers;

namespace Neo4jLiteRepo.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class NodePropertyAttribute(string propertyName, bool exclude = false) : Attribute
    {
        /// <summary>
        /// Exclude the property from being added to the graph for this Label
        /// </summary>
        /// <remarks>useful if a base class writes a property by default that
        /// is not needed or appropriate on an inheriting class.</remarks>
        public bool Exclude { get; } = exclude;

        /// <summary>
        /// Name of the Node's property, returned with Graph Property Casing
        /// </summary>
        /// <remarks>may want to add the ability to not use ToGraphPropertyCasing,
        /// but only if really needed, using it is good practice for graph property names.</remarks>
        public string PropertyName { get; } = propertyName.ToGraphPropertyCasing();
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class BoolNodePropertyAttribute(string propertyName, bool defaultValue, bool exclude = false) 
        : NodePropertyAttribute(propertyName, exclude)
    {
        public bool NullDefault { get; } = defaultValue;

    }
}  