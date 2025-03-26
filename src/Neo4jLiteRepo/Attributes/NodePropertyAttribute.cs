using Neo4jLiteRepo.Helpers;

namespace Neo4jLiteRepo.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class NodePropertyAttribute(string propertyName) : Attribute
    {
        /// <summary>
        /// Name of the Node's property, returned with Graph Property Casing
        /// </summary>
        /// <remarks>may want to add the ability to not use ToGraphPropertyCasing,
        /// but only if really needed, using it is good practice for graph property names.</remarks>
        public string PropertyName { get; } = propertyName.ToGraphPropertyCasing();
    }
}